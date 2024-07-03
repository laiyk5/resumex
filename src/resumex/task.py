class Task:
    """
    Encapsulates a function call with its arguments and keyword arguments,
    allowing it to be executed as a single callable entity.

    Attributes:
        name (str): The name of the step.

    """

    def __init__(self, func, args: tuple = (), kwargs: dict = {}, name=None):
        """
        Args:
            func (callable): The function to be called.
            args (tuple, optional): The positional arguments to be passed to the function. Defaults to ().
            kwargs (dict, optional): The keyword arguments to be passed to the function. Defaults to {}.
            name (str, optional): The name of the step. If not provided, a default name is generated using the function's name and the hash of the instance.
        """
        self._func = func
        self._args = args
        self._kwargs = kwargs

        self.name = name if name is not None else f"{func.__name__}_{str(hash(self))}"

    def __call__(self):
        return self._func(*self._args, **self._kwargs)

    def __str__(self) -> str:
        return 'Step "self.name"'

    def __repr__(self) -> str:
        return f"_Step(func={self._func!r}, args={self._args!r}, kwargs={self._kwargs!r}, name={self.name!r})"
