from datetime import datetime, timedelta
from functools import wraps

from chalicelib.logger import ERROR, EXCEPTION, log


def safe_timed_call(default_value=None, max_time=timedelta(minutes=12)):
    """Call a function and return a default value it raises an exception
    or if, after the first call, it takes more than max_time to execute.

    Keep in mind that the time starts counting from the first call to any decorated function.
    Is not a per-function timer.

    The time is checked before the function is called, so the function will not be interrupted
    in the middle of its execution.

    Specially useful when the same function is called multiple "short" times in the same AWS lambda
    execution, to avoid time limits.
    """

    def decorator(func):
        first_call_time = datetime.now()
        execution_times = []

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal first_call_time

            current_time = datetime.now()

            avg_execution_time = (
                sum(execution_times) / len(execution_times) if execution_times else 0
            )
            elapsed_time = current_time - first_call_time
            estimated_time_needed = elapsed_time + timedelta(seconds=avg_execution_time)
            if estimated_time_needed >= max_time:
                value = default_value() if callable(default_value) else default_value
                log(
                    "SAFE_TIMED_CALL",
                    ERROR,
                    "INTERNAL_CLOSE_TO_TIMEOUT",
                    {
                        "function": func.__name__,
                        "max_time": max_time,
                        "default_value": value,
                    },
                )
                return value

            try:
                return func(*args, **kwargs)
            except Exception as e:
                value = default_value() if callable(default_value) else default_value
                log(
                    "SAFE_TIMED_CALL",
                    EXCEPTION,
                    "INTERNAL_EXCEPTION",
                    {
                        "function": func.__name__,
                        "exception": str(e),
                        "default_value": value,
                    },
                )
                return value
            finally:
                execution_times.append((datetime.now() - current_time).total_seconds())

        return wrapper

    return decorator
