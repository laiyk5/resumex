import asyncio
import logging
import time
from logging import FileHandler, StreamHandler
from multiprocessing import freeze_support

from resume import JobGraph, MPExecutor


def busy_wait(dt):
    current_time = time.time()
    while time.time() < current_time + dt:
        pass


SLEEPTIME = 0.1


def fn_1(input):
    res = {
        "2": None,
        "3": None,
    }
    busy_wait(SLEEPTIME)
    return res


def fn_2(input):
    res = {"4": None, "5": None}
    busy_wait(SLEEPTIME)
    return res


def fn_3(input):
    res = {"5": None, "6": None}
    busy_wait(SLEEPTIME)
    return res


def fn_4(input):
    res = {"7": None}
    busy_wait(SLEEPTIME)
    return res


def fn_5(input):
    res = {"7": None}
    busy_wait(SLEEPTIME)
    return res


def fn_6(input):
    res = {"7": None}
    busy_wait(SLEEPTIME)
    return res


def fn_7(input):
    res = {"__end__": None}
    busy_wait(SLEEPTIME)
    return res


NUMBER_OF_ROUNDS = 30


async def main(executor: MPExecutor, rounds):
    tasks = []
    for _ in range(rounds):
        co = asyncio.create_task(executor.arun({"1": None}))
        tasks.append(co)

    completed, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

    return completed


if __name__ == "__main__":
    freeze_support()

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

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(processName)s\t - %(levelname)s - %(message)s"
    )

    console_handler = StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)

    file_handler = FileHandler(".log")
    file_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger("resume")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    task_fn = {
        "1": fn_1,
        "2": fn_2,
        "3": fn_3,
        "4": fn_4,
        "5": fn_5,
        "6": fn_6,
        "7": fn_7,
    }

    executor = MPExecutor(job, task_fn)

    """
    arun: 2.825, 2.767, 2.757, 2.742, 2.723
        avg: 2.715
        std: 0.019
    run_list: 2.688, 2.698, 2.719, 2.738, 2.730
        avg: 2.715
        avg: 0.019
    """
    begin_multi = time.time()
    asyncio.run(main(executor, NUMBER_OF_ROUNDS))
    end_multi = time.time()

    begin_single = time.time()
    asyncio.run(main(executor, 1))
    end_single = time.time()

    elapsed_single = end_single - begin_single
    elapsed_multi = end_multi - begin_multi
    print(f"{elapsed_single}s elapsed")
    print(f"{elapsed_multi}s elapsed")
    acc_rate = elapsed_single * NUMBER_OF_ROUNDS / (elapsed_multi + 0.01)
    print(f"acceleration rate: {acc_rate}, {acc_rate:.3f}")

    executor.shutdown()
