import functools


def ensure_list(f):
    @functools.wraps(f)
    def wrapper(cls, records, *args, **kwargs):
        if not isinstance(records, list):
            records = [records]
        return f(cls, records, *args, **kwargs)

    return wrapper
