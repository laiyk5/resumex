import datetime
import logging
import os
import typing as t
from random import random

import graphviz
import jsonlines
from pydantic import BaseModel

from .job_graph import JobGraph
from .scheduler import Scheduler
from .task import Task

logger = logging.getLogger(__name__)


class _Record(BaseModel):
    task_name: str
    timestamp: str = datetime.datetime.now().strftime(r"%d/%m/%Y,%H:%M:%S")


class Executor:
    scheduler: Scheduler
    tasks: t.Dict[str, Task]
    finished_tasks: t.List[_Record] = []

    def __init__(self, scheduler, tasks, cache_fname=None) -> None:
        self.scheduler = scheduler
        self.tasks = tasks

        self.__dot = self.scheduler.job.draw_graph(render=False)

        self.cache_fname = (
            cache_fname if cache_fname is not None else self.__get_cache_fname()
        )
        os.makedirs(os.path.dirname(self.cache_fname), exist_ok=True)

        if os.path.exists(self.cache_fname):
            self.__load_cache()

    def __load_cache(self):
        with jsonlines.open(self.cache_fname, "r") as reader:
            for line in reader:
                record = _Record(**line)
                self.finished_tasks.append(record)
                scheduler.commit(record.task_name)
                self.__dot.node(record.task_name, color="green")

    def __add_cache(self, task_name):
        record = _Record(task_name=task_name)
        self.finished_tasks.append(record)
        with jsonlines.open(self.cache_fname, "a") as writer:
            writer.write(record.model_dump())

    def __get_cache_fname(self):
        return os.path.join("_cache", "resume", self.scheduler.job.name + ".jsonl")

    def run(self):
        while True:
            task_name = self.scheduler.schedule()
            if self.scheduler.is_finished:
                break
            assert task_name is not None
            self.__dot.node(task_name, color="red")

            self.tasks[task_name]()
            self.scheduler.commit(task_name)

            self.__add_cache(task_name)
            self.__dot.node(task_name, color="green")

    def draw_graph(self) -> graphviz.Digraph:
        """Draw the graph in graphviz.Digraph.

        The graph is updated as execution goes. Green indicates success,
        Red means failure.

        Returns:
            graphviz.Digraph: the graph.
        """
        return self.__dot


if __name__ == "__main__":
    job = JobGraph("greate")
    job.add_edge("1", "2")
    job.add_edge("1", "3")
    job.add_edge("2", "4")
    job.add_edge("2", "5")
    job.add_edge("3", "5")
    job.add_edge("3", "6")
    job.add_edge("4", "7")
    job.add_edge("5", "7")
    job.add_edge("6", "7")
    job.draw_graph(render=True)
    scheduler = Scheduler(job)

    def madman(i):
        if random() < 0.2:
            raise RuntimeError
        print(i)

    tasks = {f"{i}": Task(lambda i=i: madman(i)) for i in range(1, 8)}

    executor = Executor(scheduler, tasks)
    try:
        executor.run()
    except:
        pass
    finally:
        graph = executor.draw_graph()
        graph.render(f"JobGraph-{executor.scheduler.job.name}")
