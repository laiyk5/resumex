import json
import time
from typing import Callable, List, Dict, Any, Generator
import os

def read_jsonl(file_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(file_path):
        return []
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]

def write_jsonl(file_path: str, data: List[Dict[str, Any]]):
    with open(file_path, 'w') as file:
        for item in data:
            file.write(json.dumps(item) + '\n')

def append_jsonl(file_path: str, data: List[Dict[str, Any]]):
    with open(file_path, 'a') as file:
        for item in data:
            file.write(json.dumps(item) + '\n')
            

class IntervalModifier:
    def __init__(self, file_path, interval=60):
        self.last_saved_time = time.time()
        self.interval = 1
        self.file_path = file_path
        self.new_entries = {}
    
    def stage(self, lineno, obj):
        self.new_entries[lineno] = obj
        
    def commit_interval(self):
        current_time = time.time()
        if current_time - self.last_saved_time >= self.interval:
            self.commit()
            self.last_saved_time = current_time
        
    def commit(self):
        if not self.new_entries:
            return
        
        with open(self.file_path, 'r') as file:
            lines = file.readlines()
        
        # Modify specified lines
        for line_number, new_data in self.new_entries.items():
            if 0 <= line_number < len(lines):
                lines[line_number] = json.dumps(new_data) + '\n'
            else:
                raise IndexError(f"Line number {line_number} is out of range.")
        
        # Write the modified lines back to the file
        with open(self.file_path, 'w') as file:
            file.writelines(lines)
            print("writes!")

class IntervalAppender:
    def __init__(self, file_path, interval=60):
        self.last_saved_time = time.time()
        self.interval = 1
        self.file_path = file_path
        self.new_entries = []
        
    def stage(self, obj):
        self.new_entries.append(obj)
        
    def commit_interval(self):
        current_time = time.time()
        if current_time - self.last_saved_time >= self.interval:
            self.commit()
            self.last_saved_time = current_time
        
    def commit(self):
        """Function to save all new entries to the file."""
        if self.new_entries:
            append_jsonl(self.file_path, self.new_entries)
            self.new_entries.clear()
            print("saved!")
            

def resume_jsonl(
    file_path: str, 
    f: Callable[..., Any], 
    *arg_generators: Generator, 
    interval: int = 60, 
    retry: bool = False, 
    should_retry: Callable[[Any], bool] = lambda x: False
):
    """
    Process a JSONL file by applying function f to each entry and handling retries.

    :param file_path: Path to the JSONL file.
    :param f: Function to generate new JSON objects.
    :param arg_generators: Generators that produce arguments for the function f.
    :param interval: Time interval (in seconds) between saving results.
    :param retry: Whether to retry based on function should_retry.
    :param should_retry: Function to determine if the result from f should be retried.
    """

    # Read existing data
    data = read_jsonl(file_path)
    
    modifier = IntervalModifier(file_path, interval)

    # Handle retries
    for lineno, entry in enumerate(data):
        try:
            args = [next(gen) for gen in arg_generators]
        except StopIteration:
            break  # Exit the loop if any generator is exhausted
        
        if retry and should_retry(entry):
            result = f(*args)
            modifier.stage(lineno, result)
            modifier.commit_interval()
    modifier.commit()
    
    saver = IntervalAppender(file_path, interval)

    # Continuously generate and save new data
    while True:
        try:
            args = [next(gen) for gen in arg_generators]
        except StopIteration:
            break  # Exit the loop if any generator is exhausted

        result = f(*args)
        saver.stage(result)
        saver.commit_interval()

    # Ensure any remaining entries are saved at the end
    saver.commit()

