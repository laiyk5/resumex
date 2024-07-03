import logging
import logging.handlers
import multiprocessing as mp
import os
import typing as t

from .job_graph import JobGraph
from .task import Task

STOP = "__STOP__"

logger = logging.getLogger(__name__)

def worker(fn:t.Callable[[dict], dict], src_queues: dict[str, mp.Queue], dest_queues: dict[str, mp.Queue], log_queue: mp.Queue):
    qh = logging.handlers.QueueHandler(log_queue)
    
    logger.addHandler(qh)
    logger.setLevel(logging.DEBUG)
    
    while True:
        src_datas = {task: queue.get() for task, queue in src_queues.items()}
        if STOP in src_datas.values():
            for dest, queue in dest_queues.items():
                queue.put(STOP)
            return
        
        dest_datas = fn(src_datas)
        logger.debug(f"prev: {list(src_queues.keys())}\tnext: {list(dest_queues.keys())}\tsrc_data: {src_datas}\tdest_data: {dest_datas}")
        for dest, data in dest_datas.items():
            dest_queues[dest].put(data)
    
class MPExecutor:
    """Multi-Progessing Executor

    job is described as a digraph, the digraph is then traversed and queues
    between each task is created.
    1. The worker function wrap the task, taking two dictionary
    of queues: one for source group, one for dest group, mapping task name
    into queue. The task should also take two dictionary of inputs, one for
    source and one for dest, mapping task name into python object.
    """

    job: JobGraph
    task_fn: dict[str, t.Callable]

    src_queues: dict[str, dict[str, mp.Queue]]
    dest_queues: dict[str, dict[str, mp.Queue]]
    processes: list[mp.Process]

    def __init__(self, job: JobGraph, task_fn: dict[str, t.Callable]):
        self.job = job
        self.task_fn = task_fn

        # initialize (src/dest)-queue mapping for each task.
        self.__init_queues()

        # initialize processes
        self.__init_processes()

    def __init_queues(self):
        tasks = [self.job.begin_task] + self.job.serialize()

        self.src_queues: dict[str, dict[str, mp.Queue]] = {}
        self.dest_queues: dict[str, dict[str, mp.Queue]] = {}

        for task in tasks + [self.job.end_task]:
            self.src_queues[task] = {}
            self.dest_queues[task] = {}

        for task in tasks:
            dest_set = self.job.next[task]
            for dest in dest_set:
                q = mp.Queue()
                self.dest_queues[task][dest] = q
                self.src_queues[dest][task] = q

    def __init_processes(self):
        logging_queue = mp.Queue()
        
        listener = logging.handlers.QueueListener(logging_queue, *logger.handlers)  
        listener.start()
        
        self.processes = [
            mp.Process(
                target=worker,
                args=(
                    self.task_fn[task],
                    self.src_queues[task],
                    self.dest_queues[task],
                    logging_queue
                ),
            )
            for task in self.job.node - set([self.job.begin_task, self.job.end_task])
        ]
        
        for p in self.processes:
            p.start()

    def run(self, input: dict[str, t.Any]):
        for task, value in input.items():  # a graph might have multiple inputs.
            self.dest_queues[self.job.begin_task][task].put(value)
            
        if STOP in input.values():
            return {}

        def __print_get(task, queue):
            ret = queue.get()
            logger.debug(f"GETTING {task}: {ret}")
            return ret
        result = {
            task: __print_get(task, queue)
            for task, queue in self.src_queues[self.job.end_task].items()
        }

        return result

    def shutdown(self):
        input_tasks = self.job.next[self.job.begin_task]
        inputs = {task: STOP for task in input_tasks}
        self.run(inputs)
        for p in self.processes:
            p.join()

