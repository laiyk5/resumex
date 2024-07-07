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

    _node: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]
    _next: Dict[str, set[str]]
    _prev: Dict[str, set[str]]

    def __init__(self, name: str):
        self.name = name

        self._node = dict()
        self._next = dict()
        self._prev = dict()

        self._begin_task = BEGIN_TASK_NAME
        self._end_task = END_TASK_NAME

        self._node[self._begin_task] = _placeholder_task
        self._node[self._end_task] = _placeholder_task

        self._prev[self._begin_task] = set()
        self._next[self._begin_task] = set()
        self._prev[self._end_task] = set()
        self._next[self._end_task] = set()

    def __link_nodes(self, task1: str, task2: str):
        self._next[task1].add(task2)
        self._prev[task2].add(task1)

    def __unlink_nodes(self, task1: str, task2: str):
        self._next[task1].remove(task2)
        self._prev[task2].remove(task1)

    def add_node(self, task: str, fn: Callable[[Dict[str, Any]], Dict[str, Any]]):
        if task in self._node.keys():
            msg = f"task {task} already exists."
            raise ValueError(msg)

        self._node[task] = fn

        self._next[task] = set()
        self._prev[task] = set()

        # create default links
        self.__link_nodes(self._begin_task, task)
        self.__link_nodes(task, self._end_task)

    def add_edge(self, task1: str, task2: str):

        # Remove default edges
        if self._end_task in self._next[task1]:
            self.__unlink_nodes(task1, self._end_task)

        if self._begin_task in self._prev[task2]:
            self.__unlink_nodes(self._begin_task, task2)

        self.__link_nodes(task1, task2)

        return self

    def serialize(self) -> List[str]:
        serialization = []
        visited = set()
        dependencies = deque([self._end_task])
        while dependencies:
            head = dependencies.popleft()
            if head in visited:
                continue
            serialization.append(head)
            visited.add(head)

            for d in self._prev[head]:
                if d not in visited:
                    dependencies.append(d)

        serialization.remove(self._begin_task)
        serialization.remove(self._end_task)
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

        for node in self._node:
            dot.node(node)

        for node, edges in self._next.items():
            for edge in edges:
                dot.edge(node, edge)

        if render:
            if filename is None:
                filename = f"JobGraph_{self.name}"
            dot.render(filename, view=False)
        return dot
