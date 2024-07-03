import logging
from logging import FileHandler, StreamHandler, handlers
from multiprocessing import freeze_support
from time import sleep
from resume import mp_executor
from resume.job_graph import JobGraph
from resume.mp_executor import MPExecutor



SLEEPTIME = 2
def fn_1(input):
    res = {
        '2': None,
        '3': None,
    }
    sleep(SLEEPTIME)
    return res


def fn_2(input):
    res = {
        '4': None,
        '5': None
    }
    sleep(SLEEPTIME)
    return res


def fn_3(input):
    res = {
        '5': None,
        '6': None
    }
    sleep(SLEEPTIME)
    return res


def fn_4(input):
    res = {
        '7': None
    }
    sleep(SLEEPTIME)
    return res


def fn_5(input):
    res = {
        '7': None
    }
    sleep(SLEEPTIME)
    return res


def fn_6(input):
    res = {
        '7': None
    }
    sleep(SLEEPTIME)
    return res


def fn_7(input):
    res = {
        '__end__': None
    }
    sleep(SLEEPTIME)
    return res


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
    
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(processName)s\t - %(levelname)s - %(message)s")
    
    console_handler = StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    
    file_handler = FileHandler('.log')
    file_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    
    logger = mp_executor.logger
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

    result = executor.run({"1": None})
    result = executor.run({"1": None})
    result = executor.run({"1": None})
    result = executor.run({"1": None})
    print(f"{__name__}: {result}")
    executor.shutdown()
    