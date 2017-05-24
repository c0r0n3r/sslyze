# -*- coding: utf-8 -*-
"""Generic, simple thread pool used in some of the plugins.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import threading

try:
    # Python 3
    # noinspection PyCompatibility
    from queue import Queue
    from queue import Empty as Empty
except ImportError:
    # Python 2
    # noinspection PyCompatibility
    from Queue import Queue
    from Queue import Empty as Empty


class ThreadPool(object):
    """Generic Thread Pool used in some of the plugins.
    Any unhandled exception happening in the work function goes to the error
    queue that can be read using get_error().
    Anything else goes to the result queue that can be read using get_result().
    """
    def __init__(self):
        self._active_jobs = 0
        self._job_q = Queue()
        self._result_q = Queue()
        self._thread_list = []

    def add_job(self, job):
        self._job_q.put(job)

    def get_error(self):
        return self._get_result_by_type(error=True)

    def get_result(self):
        return self._get_result_by_type(error=False)

    def _get_result_by_type(self, error):
        remaining_results = []
        while self._active_jobs:
            result = self._result_q.get()
            self._result_q.task_done()
            self._active_jobs -= 1

            if error == isinstance(result[1], Exception):
                yield result
            else:
                remaining_results.append(result)

        for result in remaining_results:
            self._result_q.put(result)
            self._active_jobs += 1

    def start(self, nb_threads):
        """
        Should only be called once all the jobs have been added using add_job().
        """
        if self._active_jobs != 0:
            raise Exception('Threads already started.')

        self._active_jobs = self._job_q.qsize()

        # Create thread pool
        for _ in range(nb_threads):
            worker = threading.Thread(
                target=_work_function,
                args=(self._job_q, self._result_q))
            worker.start()
            self._thread_list.append(worker)

    def join(self):
        # Clean exit
        self._job_q.join()
        [worker.join() for worker in self._thread_list]
        self._result_q.join()
        self._active_jobs = 0


def _work_function(job_q, result_q):
    """Work function expected to run within threads."""
    while True:
        try:
            job = job_q.get(False)
        except Empty:
            break

        function = job[0]
        args = job[1]
        try:
            result = function(*args)
        except Exception as e:
            result_q.put((job, e))
        else:
            result_q.put((job, result))
        finally:
            job_q.task_done()
