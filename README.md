![resumex logo](https://raw.githubusercontent.com/laiyk5/resumex/main/docs/source/_static/ResumeX.png)

# ResumeX
[![Upload Python Package](https://github.com/laiyk5/resumex/actions/workflows/python-publish.yml/badge.svg)](https://github.com/laiyk5/resumex/actions/workflows/python-publish.yml)
[![Deploy Sphinx documentation to Pages](https://github.com/laiyk5/resumex/actions/workflows/doc-publish.yml/badge.svg?branch=main)](https://github.com/laiyk5/resumex/actions/workflows/doc-publish.yml)

A python package tha handle common issues in data processing.

## Links

- [github](https://github.com/laiyk5/resumex)
- [doc](https://laiyk5.github.io/resumex)

## Usage

1. Organize you tasks into a job, which is represented by `JobGraph`
2. Define your task as a function, which has input as a mapping from source-task-name to input data, and output as a dict from dest-task-name to output data.
3. Run the function with several inputs asynchronously with `MPExecutor`
