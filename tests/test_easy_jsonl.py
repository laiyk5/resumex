import time
from resumex.resume import resume_jsonl
import random

def example_f(id, param1, param2):
    # time.sleep(1)
    return {
        "id": id,
        "param1": param1,
        "param2": param2,
        "timestamp": time.time()
    }

def retry_decision(result):
    # Example retry decision function
    return result["param1"] < 0.9

length = 100
param1_gen = (random.random() for _ in range(length))
param2_gen = (random.randint(0, 100) for _ in range(length))
id_gen = (i for i in range(length))

resume_jsonl(
    'example.jsonl', 
    example_f,
    id_gen,
    param1_gen, 
    param2_gen, 
    interval=1, 
    retry=True, 
    should_retry=retry_decision
)
