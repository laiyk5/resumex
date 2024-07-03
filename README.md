# ResumeX

A python package tha handle common issues in data processing.

## Usage

1. Organize you tasks into a job, which is represented by `JobGraph`
2. Define your task as a function, which has input as a mapping from source-task-name to input data, and output as a dict from dest-task-name to output data.
3. Run the function with several inputs asynchronously with `MPExecutor`
