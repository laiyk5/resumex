from .step import Step


from collections import deque
from typing import Dict, List
import graphviz


class GraphTask:
    """
    A class to represent a directed acyclic graph (DAG) of tasks, where nodes are tasks (steps)
    and edges represent dependencies between these tasks.

    Attributes:
        node (Dict[str, Step]): A dictionary of nodes where keys are node names and values are Step objects.
        edge (Dict[str, Set[str]]): A dictionary of edges where keys are node names and values are sets of node names that the key node depends on.
        _begin_node (Step): A special node representing the beginning of the graph.
        _end_node (Step): A special node representing the end of the graph.
        
    """

    node: Dict[str, Step]
    edge: Dict[str, set[str]]

    def __init__(self, name):
        self.name = name

        self.node = {}
        self.edge = {}
        self._begin_node = Step(lambda: None, name="_begin")
        self._end_node = Step(lambda: None, name="_end")

        self.node[self._begin_node.name] = self._begin_node
        self.node[self._end_node.name] = self._end_node
        self.edge[self._end_node.name] = {self._begin_node.name}
        self.edge[self._begin_node.name] = set()  # should always be an empty set.

    def add_node(self, step: Step):
        """
        Adds a new node (Step) to the graph.

        Args:
            step (Step): The Step object to be added to the graph.
        """
        if step.name in self.node:
            return

        self.node[step.name] = step
        self.edge[self._end_node.name].add(step.name)
        self.edge[step.name] = {self._begin_node.name}

    def add_edge(self, step1: Step, step2: Step):
        """
        Adds an edge from step1 to step2, indicating that step1 depends on step2.

        Args:
            step1 (Step): The dependent Step object.
            step2 (Step): The Step object that step1 depends on.
        """

        if step1.name not in self.node.keys():
            self.add_node(step1)
        if step2.name not in self.node.keys():
            self.add_node(step2)

        # Remove default dependancy from step1 to self._begin_node if it exists.
        if (
            len(self.edge[step1.name]) == 1
            and list(self.edge[step1.name])[0] == self._begin_node.name
        ):
            self.edge[step1.name].clear()

        # Remove default dependancy from self._end_node to step2 if it exists.
        if step2.name in self.edge[self._end_node.name]:
            self.edge[self._end_node.name].remove(step2.name)

        self.edge[step1.name].add(step2.name)
        
    def serialize(self) -> List[Step]:
        """
        Serializes the graph into a list of steps in a topological order,
        ensuring all dependencies of a node appear before the node itself in the list.

        Returns:
            List[Step]: A list of Step objects in topological order.
        """
        serialization = []
        visited = set()
        dependencies = deque([self._end_node.name])
        while dependencies:
            head = dependencies.popleft()
            if head in visited:
                continue
            serialization.append(head)
            visited.add(head)

            for d in self.edge[head]:
                if d not in visited:
                    dependencies.append(d)

        serialization.remove(self._begin_node.name)
        serialization.remove(self._end_node.name)
        serialization.reverse()
        return list(map(lambda name: self.node[name], serialization))
    
    def draw_graph(self, render:bool=False, filename=None):
        """Draw graph using graphviz.

        Args:
            render (bool, optional): whether to render the graph. Defaults to False.
            filename (_type_, optional): The path to store the rendered graph. Defaults to None.

        Returns:
            graphviz.Digraph: _description_
        """
        dot = graphviz.Digraph(comment=self.name)
        
        for node in self.node.keys():
            dot.node(node)
        
        for node, edges in graph_task.edge.items():
            for edge in edges:
                dot.edge(node, edge)
        
        if render:
            if filename is None:
                filename = f"GraphTask_{self.name}"
            dot.render(filename, view=False)
        return dot


# Example usage
if __name__ == "__main__":

    graph_task = GraphTask("anonymous")

    step1 = Step(lambda: print("Step 1"), name="step1")
    step2 = Step(lambda: print("Step 2"), name="step2")
    step3 = Step(lambda: print("Step 3"), name="step3")

    graph_task.add_node(step1)
    graph_task.add_node(step2)
    graph_task.add_node(step3)

    graph_task.add_edge(step2, step1)  # step2 depends on step1
    graph_task.add_edge(step3, step2)  # step3 depends on step2

    dot = graph_task.draw_graph(render=True)
    
    print(graph_task.serialize())
    print(repr(step1))
    print(step1)
