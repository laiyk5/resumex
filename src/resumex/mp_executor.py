import asyncio
import logging
import logging.handlers
import multiprocessing as mp
import typing as t

from .job_graph import JobGraph

STOP = "__STOP__"

logger = logging.getLogger(__name__)


def worker(
    fn: t.Callable[[dict], dict],
    src_queues: dict[str, mp.Queue],
    dest_queues: dict[str, mp.Queue],
    log_queue: mp.Queue,
):
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

        assert dest_datas.keys() == dest_queues.keys()

        logger.debug(
            f"prev: {list(src_queues.keys())}\tnext: {list(dest_queues.keys())}\tsrc_data: {src_datas}\tdest_data: {dest_datas}"
        )
        for dest, data in dest_datas.items():
            dest_queues[dest].put(data)


class MPExecutor:
    """Multi-Progessing Executor
    """

    _job: JobGraph
    _task_fn: dict[str, t.Callable[[dict], dict]]

    _src_queues: dict[str, dict[str, mp.Queue]]
    _dest_queues: dict[str, dict[str, mp.Queue]]
    _processes: list[mp.Process]

    def __init__(self, job: JobGraph, task_fn: dict[str, t.Callable[[dict], dict]]):
        """_summary_

        Args:
            job (JobGraph): job is described as a digraph, the digraph is then traversed and queues between each task is created.
            task_fn (dict[str, t.Callable[[dict], dict]]): task function sould accept a dict mapping input taskname to input data, and return a dict mapping output taskname to output data.
        """
        self._job = job
        self._task_fn = task_fn

        # initialize (src/dest)-queue mapping for each task.
        self.__init_queues()

        # initialize processes
        self.__init_processes()

    def __init_queues(self):
        tasks = [self._job.begin_task] + self._job.serialize()

        self._src_queues: dict[str, dict[str, mp.Queue]] = {}
        self._dest_queues: dict[str, dict[str, mp.Queue]] = {}

        for task in tasks + [self._job.end_task]:
            self._src_queues[task] = {}
            self._dest_queues[task] = {}

        for task in tasks:
            dest_set = self._job.next[task]
            for dest in dest_set:
                q = mp.Queue()
                self._dest_queues[task][dest] = q
                self._src_queues[dest][task] = q

    def __init_processes(self):
        logging_queue = mp.Queue()

        listener = logging.handlers.QueueListener(logging_queue, *logger.handlers)
        listener.start()

        self._processes = [
            mp.Process(
                target=worker,
                args=(
                    self._task_fn[task],
                    self._src_queues[task],
                    self._dest_queues[task],
                    logging_queue,
                ),
            )
            for task in self._job.node - set([self._job.begin_task, self._job.end_task])
        ]

        for p in self._processes:
            p.start()

    def run(self, input: dict[str, t.Any]) -> dict[str, t.Any]:
        """run on the input and block until all the output task generate results.

        Args:
            input (dict[str, Any]): a dictionary that map input task name to the input data

        Returns:
            dict[str, Any]: a dictionary that map output task name to the output data.
        """
        for task, value in input.items():  # a graph might have multiple inputs.
            self._dest_queues[self._job.begin_task][task].put(value)

        if STOP in input.values():
            return {}

        def __print_get(task: str, queue: mp.Queue) -> t.Any:
            ret = queue.get()
            logger.debug(f"GETTING {task}: {ret}")
            return ret

        result = {
            task: __print_get(task, queue)
            for task, queue in self._src_queues[self._job.end_task].items()
        }

        return result

    async def arun(self, input: dict[str, t.Any]) -> dict[str, t.Any]:
        """async version of `run`

        Args:
            input (dict[str, t.Any]): a dictionary that map input task name to the input data

        Returns:
            Coroutine: a coroutine that wrap output of `run`.
        """
        return await asyncio.to_thread(self.run, input)

    def shutdown(self):
        """join all the subprocesses."""
        input_tasks = self._job.next[self._job.begin_task]
        inputs = {task: STOP for task in input_tasks}
        self.run(inputs)
        for p in self._processes:
            p.join()
