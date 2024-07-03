from collections import deque
from typing import Set

from .job_graph import JobGraph


class Scheduler:
    job: JobGraph
    finished: Set[str] = set()
    candidates: deque[str] = deque()

    is_finished: bool = False

    def __init__(self, job: JobGraph):
        self.job = job

        self.commit(self.job.begin_task)

    def commit(self, step: str):
        """declares a step has finished.
        Args:
            step (str): the id of the step.
        """

        if step in self.finished:
            return

        self.finished.add(step)

        if step in self.candidates:
            self.candidates.remove(step)

        for next_step in self.job.next[step]:
            if self.job.prev[next_step] - self.finished:
                continue  # The step is not ready to run.
            if next_step in self.candidates:
                continue  # Already added to candidates.
            if next_step in self.finished:
                continue
            self.candidates.append(next_step)

    def schedule(self) -> str | None:
        """schedule a step that is able to be executed.

        Returns:
            str | None: schedule a task or if no task is available, return None.
        """
        if len(self.candidates) != 0:
            step = self.candidates.popleft()
            if step == self.job.end_task:
                self.is_finished = True
                return None
            return step
        else:
            return None


if __name__ == "__main__":
    taskgraph = JobGraph("greate")
    taskgraph.add_edge("1", "2")
    taskgraph.add_edge("1", "3")
    taskgraph.add_edge("2", "4")
    taskgraph.add_edge("2", "5")
    taskgraph.add_edge("3", "5")
    taskgraph.add_edge("3", "6")
    taskgraph.add_edge("4", "7")
    taskgraph.add_edge("5", "7")
    taskgraph.add_edge("6", "7")
    scheduler = Scheduler(taskgraph)

    while True:
        step = scheduler.schedule()
        if step == None:
            break
        scheduler.commit(step)
        print(step)
