import timeit

setup = """
class RenderJob:
    def __init__(self, i, status):
        self.i = i
        self.status = status
    def to_dict(self):
        return {"id": self.i, "status": self.status, "submitted_epoch": self.i}

import random
random.seed(42)
jobs = {i: RenderJob(i, "Completed" if random.random() > 0.5 else "Pending") for i in range(10000)}
"""

test_old = """
all_jobs = [j.to_dict() for j in jobs.values()]
active  = [d for d in all_jobs if d.get("status") != "Completed"]
done    = [d for d in all_jobs if d.get("status") == "Completed"]
"""

test_new = """
active = []
done = []
for j in jobs.values():
    d = j.to_dict()
    if d.get("status") == "Completed":
        done.append(d)
    else:
        active.append(d)
"""

n = 100
t_old = timeit.timeit(test_old, setup=setup, number=n)
t_new = timeit.timeit(test_new, setup=setup, number=n)

print(f"Old approach: {t_old:.4f} s")
print(f"New approach: {t_new:.4f} s")
print(f"Improvement: {t_old / t_new:.2f}x faster")
