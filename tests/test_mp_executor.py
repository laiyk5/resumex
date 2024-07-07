import logging
import os
import time
from logging import FileHandler, StreamHandler
from multiprocessing import freeze_support

from resumex.job import JobGraph, MPExecutor

from resumex.job.job_graph import BEGIN_TASK_NAME

SLEEPTIME = 0.1

def fn_1(input):
    res = {
        "2": input[BEGIN_TASK_NAME],
        "3": input[BEGIN_TASK_NAME],
    }
    time.sleep(SLEEPTIME)
    return res


def fn_2(input):
    res = {"4": input["1"], "5": input["1"]}
    time.sleep(SLEEPTIME)
    return res


def fn_3(input):
    res = {"5": input["1"], "6": input["1"]}
    time.sleep(SLEEPTIME)
    return res


def fn_4(input):
    res = {"7": input["2"]}
    time.sleep(SLEEPTIME)
    return res


def fn_5(input):
    res = {"7": input["2"]}
    time.sleep(SLEEPTIME)
    return res


def fn_6(input):
    res = {"7": input["3"]}
    time.sleep(SLEEPTIME)
    return res


def fn_7(input):
    res = {"__end__": list(input.values())}
    time.sleep(SLEEPTIME)
    return res


NUMBER_OF_ROUNDS = 30


if __name__ == "__main__":
    freeze_support()

    out_dpth = os.path.join("out", "tests")
    os.makedirs(out_dpth, exist_ok=True)

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

    job = JobGraph("greate")

    job.add_node("1", fn_1)
    job.add_node("2", fn_2)
    job.add_node("3", fn_3)
    job.add_node("4", fn_4)
    job.add_node("5", fn_5)
    job.add_node("6", fn_6)
    job.add_node("7", fn_7)

    job.add_edge("1", "2")
    job.add_edge("1", "3")
    job.add_edge("2", "4")
    job.add_edge("2", "5")
    job.add_edge("3", "5")
    job.add_edge("3", "6")
    job.add_edge("4", "7")
    job.add_edge("5", "7")
    job.add_edge("6", "7")
    job.draw_graph(
        render=True, filename=os.path.join(out_dpth, f"test_mp_executor_job.gv")
    )

    executor = MPExecutor(job)

    begin_multi = time.time()
    results = executor.run([{"1": i} for i in range(NUMBER_OF_ROUNDS)])
    print(results)
    end_multi = time.time()

    begin_single = time.time()
    results = executor.run([{"1": 1}])
    print(results)
    end_single = time.time()

    elapsed_single = end_single - begin_single
    elapsed_multi = end_multi - begin_multi
    print(f"{elapsed_single}s elapsed")
    print(f"{elapsed_multi}s elapsed")
    acc_rate = elapsed_single * NUMBER_OF_ROUNDS / (elapsed_multi + 0.01)
    print(f"acceleration rate: {acc_rate}, {acc_rate:.3f}")

    executor.shutdown()
