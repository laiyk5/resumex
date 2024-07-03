from collections import deque
from typing import Dict, List, Set

import graphviz

from .task import Task


class JobGraph:
    """
    A Special Digraph that is always connected.
    """

    node: Set[str]
    next: Dict[str, set[str]]
    prev: Dict[str, set[str]]

    def __init__(self, name: str):
        self.name = name

        self.node = set()
        self.next = dict()
        self.prev = dict()

        self.begin_task = "__begin__"
        self.end_task = "__end__"

        self.node.add(self.begin_task)
        self.node.add(self.end_task)

        self.prev[self.begin_task] = set()
        self.next[self.begin_task] = set()
        self.prev[self.end_task] = set()
        self.next[self.end_task] = set()

    def __link_nodes(self, task1: str, task2: str):
        self.next[task1].add(task2)
        self.prev[task2].add(task1)

    def __unlink_nodes(self, task1: str, task2: str):
        self.next[task1].remove(task2)
        self.prev[task2].remove(task1)

    def add_node(self, task: str):
        if task in self.node:
            return

        self.node.add(task)

        self.next[task] = set()
        self.prev[task] = set()

        # create default links
        self.__link_nodes(self.begin_task, task)
        self.__link_nodes(task, self.end_task)

    def add_edge(self, task1: str, task2: str):

        if task1 not in self.node:
            self.add_node(task1)
        if task2 not in self.node:
            self.add_node(task2)

        # Remove default edges
        if self.end_task in self.next[task1]:
            self.__unlink_nodes(task1, self.end_task)

        if self.begin_task in self.prev[task2]:
            self.__unlink_nodes(self.begin_task, task2)

        self.__link_nodes(task1, task2)

        return self

    def serialize(self) -> List[str]:
        serialization = []
        visited = set()
        dependencies = deque([self.end_task])
        while dependencies:
            head = dependencies.popleft()
            if head in visited:
                continue
            serialization.append(head)
            visited.add(head)

            for d in self.prev[head]:
                if d not in visited:
                    dependencies.append(d)

        serialization.remove(self.begin_task)
        serialization.remove(self.end_task)
        serialization.reverse()
        return serialization

    def draw_graph(self, render: bool = False, filename=None):
        """Draw graph using graphviz.

        Args:
            render (bool, optional): whether to render the graph. Defaults to False.
            filename (_type_, optional): The path to store the rendered graph. Defaults to None.

        Returns:
            graphviz.Digraph: _description_
        """
        dot = graphviz.Digraph(comment=self.name)

        for node in self.node:
            dot.node(node)

        for node, edges in self.next.items():
            for edge in edges:
                dot.edge(node, edge)

        if render:
            if filename is None:
                filename = f"JobGraph_{self.name}"
            dot.render(filename, view=False)
        return dot


# Example usage
if __name__ == "__main__":

    graph_task = JobGraph("anonymous")

    task1 = Task(lambda: print("task 1"), name="task1")
    task2 = Task(lambda: print("task 2"), name="task2")
    task3 = Task(lambda: print("task 3"), name="task3")

    graph_task.add_edge(task1.name, task2.name)  # task1 -> task2
    graph_task.add_edge(task2.name, task3.name)  # task2 -> task3
    graph_task.add_edge(task1.name, task3.name)

    dot = graph_task.draw_graph(render=True)

    print(graph_task.serialize())
    print(repr(task1))
    print(task1)
