from .graph_task import GraphTask
from .step import Step

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Any, Callable, Tuple
import json
import os
from pydantic import BaseModel

from rich.progress import (
    TextColumn,
    Progress,
    BarColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    SpinnerColumn,
)


class _State(BaseModel):
    result: List[Any] = []
    progress: int = 0


def _load_state(state_file_path: str) -> _State:
    """
    Loads the execution state from a file.

    Args:
        state_file_path (str): The path to the state file.

    Returns:
        _State: The loaded state.
    """
    with open(state_file_path, "r") as f:
        state_dict = json.load(f)
    state = _State(**state_dict)
    return state


def _save_state(state: _State, state_file_path: str) -> None:
    """
    Saves the execution state to a file.

    Args:
        state (_State): The state to save.
        state_file_path (str): The path to the state file.
    """
    with open(state_file_path, "w") as f:
        f.write(state.model_dump_json(indent=4))


def _execute_task(
    steps_in_order: List[Step], state_file_path: str, progress: Progress, task_id: int
):
    """
    Executes the tasks in the GraphTask in topological order, resuming from a saved state if it exists.

    Args:
        task (GraphTask): The GraphTask object whose tasks will be executed.
        state_file_path (str): The path to the state file for resuming progress.
    """
    
    def _resume_linear_task(
        result: List[Any], steps: List[Step], start: int
    ) -> List[Any]:
        assert len(result) >= start

        result = result[:start]

        steps_iter = iter(steps)
        for _ in range(start):
            next(steps_iter)
        progress.update(task_id, advance=state.progress)

        try:
            for i, s in enumerate(steps_iter):
                progress.update(task_id, description=s.name)
                result.append(s())
                progress.update(task_id, advance=1)

        except Exception as e:
            print(f"Error during execution: {e}")

        return result

    if os.path.exists(state_file_path):
        state = _load_state(state_file_path)
    else:
        state = _State()

    result = _resume_linear_task(state.result, steps_in_order, state.progress)

    # Commit the progress and result.
    state.result = result
    state.progress = len(result)
    _save_state(state, state_file_path)


class Executor:
    """
    Execute a list of tasks in parallel with the ability to resume from a saved state and display progress bars for each task.

    Attributes:
        task_list (List[GraphTask]): A list of GraphTask objects to be executed.
    """

    task_list: List[GraphTask]

    def __init__(self, task_list: List[GraphTask]):
        """
        Initializes the Executor with a list of GraphTask objects.

        Args:
            task_list (List[GraphTask]): A list of GraphTask objects to be executed.
        """
        self.task_list = task_list

    def run(self, state_dpth:str):
        """run the tasks.

        Args:
            state_dpth (str): the directory where you want to store the state files.
        """
        assert os.path.exists(state_dpth)

        progress = Progress(
            TextColumn("[blue][progress.description]{task.fields[name]}", justify="left"),
            TextColumn("[progress.description]{task.description}", justify="right"),
            BarColumn(),
            "[progress.percentage]{task.completed}/{task.total}",
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            SpinnerColumn("clock"),
            # refresh_per_second=3,  # bit slower updates
        )
        with progress:
            with ThreadPoolExecutor() as executor:
                futures = {}
                for i, task in enumerate(self.task_list):
                    steps_in_order = task.serialize()
                    task_id = progress.add_task(
                        "",
                        name=f"Task {task.name}",
                        total=len(steps_in_order),
                        visible=True,
                    )
                    future = executor.submit(
                        _execute_task,
                        steps_in_order,
                        os.path.join(state_dpth, f'state_{task.name}.json'),
                        progress,
                        task_id,
                    )  # TODO: Come up with a new machenism of storing state files.
                    futures[future] = task
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"Task {task.name} generated an exception: {exc}")
                    else:
                        pass


# Example usage
if __name__ == "__main__":
    import time

    def step_func(x):
        time.sleep(3)
        return x * x

    steps = [
        Step(step_func, (1,), name="Step 1"),
        Step(step_func, (2,), name="Step 2"),
        Step(step_func, (3,), name="Step 3"),
    ]

    task_graph1 = GraphTask("Revolution")
    for step in steps:
        task_graph1.add_node(step)

    for i in range(1, len(steps)):
        task_graph1.add_edge(steps[i], steps[i - 1])

    task_graph2 = GraphTask("WW2")
    task_graph2.add_node(Step(step_func, (1,), name="Step 4"))
    task_graph2.add_node(Step(step_func, (1,), name="Step 5"))

    executor = Executor([task_graph1, task_graph2])
    
    state_dpth = '_states'
    os.makedirs(state_dpth, exist_ok=True)
    executor.run(state_dpth)
