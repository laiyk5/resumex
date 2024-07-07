from collections import deque
from typing import Any, Callable, Dict, List, Set

import graphviz

BEGIN_TASK_NAME = "__begin__"
END_TASK_NAME = "__end__"

def _placeholder_task(input):
    return {}

class JobGraph:
    """
    A Special Digraph that is always connected.
    """

    node: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]
    next: Dict[str, set[str]]
    prev: Dict[str, set[str]]

    def __init__(self, name: str):
        self.name = name

        self.node = dict()
        self.next = dict()
        self.prev = dict()

        self.begin_task = BEGIN_TASK_NAME
        self.end_task = END_TASK_NAME

        self.node[self.begin_task] = _placeholder_task
        self.node[self.end_task] = _placeholder_task

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

    def add_node(self, task: str, fn: Callable[[Dict[str, Any]], Dict[str, Any]]):
        if task in self.node.keys():
            msg = f"task {task} already exists."
            raise ValueError(msg)

        self.node[task] = fn

        self.next[task] = set()
        self.prev[task] = set()

        # create default links
        self.__link_nodes(self.begin_task, task)
        self.__link_nodes(task, self.end_task)

    def add_edge(self, task1: str, task2: str):

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
