from resumex import JobGraph

graph_task = JobGraph("anonymous")

graph_task.add_edge("task1", "task2")  # task1 -> task2
graph_task.add_edge("task2", "task3")  # task2 -> task3
graph_task.add_edge("task1", "task3")

dot = graph_task.draw_graph(render=True)

print(graph_task.serialize())
