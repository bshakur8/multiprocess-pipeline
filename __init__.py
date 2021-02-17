#!/usr/bin/env python3

import itertools
import logging
import multiprocessing
from multiprocessing.queues import JoinableQueue

__all__ = ['MultiProcessPipeline', 'logger']


def init_logger():
    handler = logging.StreamHandler()
    log_format = u'%(asctime)s [%(levelname)-1s %(process)d %(threadName)s]  %(message)s'
    handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


logger = logging.getLogger("MultiProcessPipeline")
init_logger()


class MultiProcessPipeline:

    def __init__(self, funcs, collection, default_process_num=1):
        self.collection = collection
        # communication queues
        self.queues = [JoinableQueue(maxsize=-1, ctx=multiprocessing.get_context()) for _ in range(len(funcs) + 1)]

        first_func, start_size, current_size = None, 0, 0

        self.processes = []
        for idx, data in enumerate(funcs):
            current_func, current_size = self.get_current_info(data, default_process_num)
            next_func, next_size = self.get_next_info(funcs, idx, default_process_num)

            assert callable(current_func), f"Function '{current_func}' is not a callable"

            readq, writeq = self.queues[idx], self.queues[idx + 1]
            barrier = multiprocessing.Barrier(current_size)
            start_size = start_size or max(1, current_size)
            first_func = first_func or current_func

            self.processes.append([MultiProcessPipeline.Consumer(readq, writeq, barrier, num_stops, next_func)
                                   for i, num_stops in enumerate(self.get_num_stops(current_size, next_size))])

        self.end_size = current_size
        self.start_size = start_size
        self.start_func = first_func

    @staticmethod
    def get_current_info(data, default_process_num):
        if isinstance(data, tuple):
            current_func, current_size = data
        else:
            current_func, current_size = data, default_process_num
        return current_func, current_size

    @staticmethod
    def get_next_info(funcs, idx, default_process_num):
        try:
            next_func, next_size = funcs[idx + 1]
        except TypeError:
            next_func, next_size = (funcs[idx + 1], default_process_num)
        except IndexError:
            next_func, next_size = None, None
        return next_func, next_size

    def __call__(self, ignore_results=False, *args, **kwargs):
        self.start()
        self.join(ignore_results=ignore_results)

    def start(self):
        for p in itertools.chain.from_iterable(self.processes):
            p.start()
        start_queue = self.queues[0]
        for i, item in enumerate(self.collection):
            start_queue.put(MultiProcessPipeline.Task(self.start_func, item, index=i))
        for _ in range(self.start_size):
            start_queue.put(None)

    def join(self, ignore_results=False):
        # Skip joining the last queue - no one is taking from it
        for q in self.queues[:1]:
            q.join()

        last_q = self.queues[-1]
        stops, results = 0, []
        while stops < self.end_size:
            result_task = last_q.get()
            if result_task is None:
                stops += 1
            elif not ignore_results:
                results.append(result_task)
        return (t.result for t in sorted(results, key=lambda t: t.index))

    @staticmethod
    def get_num_stops(x, y):
        integer, reminder = (1, 0) if y is None else (int(y / x), y % x)
        for i in range(x):
            yield integer + (0 if i else reminder)
        raise StopIteration

    class Task:
        def __init__(self, func, arg, index):
            self.func = func
            self.arg = arg
            self.result = arg
            self.index = index

        def __call__(self):
            if self.func:
                self.result = self.func(self.arg)
            return self

        def __str__(self):
            return f'[{self.index}] {self.result}'

    class Consumer(multiprocessing.Process):

        def __init__(self, readq, writeq, barrier, num_pills, next_func):
            super().__init__()
            self.readq = readq
            self.writeq = writeq
            self.num_pills = num_pills
            self.barrier = barrier
            self.next_func = next_func

        def run(self):
            # proc_name = self.name
            # Poison pill means shutdown
            for next_task in iter(self.readq.get, None):
                task = next_task()
                self.readq.task_done()
                self.writeq.put(MultiProcessPipeline.Task(self.next_func, task.result, task.index))

            self.readq.task_done()
            self.barrier.wait()
            for _ in range(self.num_pills):
                self.writeq.put(None)
