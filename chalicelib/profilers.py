import cProfile
import functools
import pstats
from datetime import datetime

try:
    from memory_profiler import profile as mem_profile  # noqa
except ImportError:

    def mem_profile(f):
        return f


def perf_profile(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with cProfile.Profile() as pr:
            result = f(*args, **kwargs)
        stats = pstats.Stats(pr)
        stats.sort_stats(pstats.SortKey.TIME)
        now = datetime.now().isoformat()
        stats.dump_stats(f"/tmp/{f.__name__}-{now}-.prof")
        return result

    return wrapper
