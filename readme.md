## Overview
This is an advanced, self-contained implementation of a scalable task scheduler.
It demonstrates priority-based scheduling, delayed execution, concurrency,
retry-based fault tolerance, and clean object-oriented design.

The system simulates how real-world job schedulers operate in distributed systems.

---

## Key Concepts Demonstrated
- Priority Queue (Heap) with O(log n) scheduling
- Thread-safe concurrent execution
- Delayed and scheduled task execution
- Retry-based fault tolerance
- Clean, extensible object-oriented design

---

## Single-File Implementation (Copy & Run)

```python
import time
import uuid
import heapq
import threading

# ---------------- CONFIGURATION ----------------
MAX_RETRIES = 3
WORKER_COUNT = 2
POLL_INTERVAL = 0.5


# ---------------- TASK MODEL ----------------
class Task:
    def __init__(self, priority, execution_time, run_at=None):
        self.task_id = str(uuid.uuid4())
        self.priority = priority
        self.execution_time = execution_time
        self.run_at = run_at or time.time()
        self.retries = 0

    def __lt__(self, other):
        if self.run_at == other.run_at:
            return self.priority < other.priority
        return self.run_at < other.run_at


# ---------------- THREAD-SAFE PRIORITY QUEUE ----------------
class ThreadSafePriorityQueue:
    def __init__(self):
        self.heap = []
        self.lock = threading.Lock()

    def push(self, task):
        with self.lock:
            heapq.heappush(self.heap, task)

    def pop(self):
        with self.lock:
            if self.heap:
                return heapq.heappop(self.heap)
            return None


# ---------------- SCHEDULER ----------------
class Scheduler:
    def __init__(self):
        self.queue = ThreadSafePriorityQueue()

    def schedule(self, task):
        self.queue.push(task)

    def get_ready_task(self):
        task = self.queue.pop()
        if not task:
            return None

        if task.run_at <= time.time():
            return task

        self.queue.push(task)
        return None


# ---------------- WORKER ----------------
def execute_task(task):
    try:
        print(f"[Worker] Executing task {task.task_id}")
        time.sleep(task.execution_time)

        if task.execution_time > 2:
            raise Exception("Simulated task failure")

        print(f"[Worker] Completed task {task.task_id}")

    except Exception as e:
        task.retries += 1
        print(f"[Worker] Task failed ({task.retries}): {e}")

        if task.retries < MAX_RETRIES:
            return task

        print(f"[Worker] Task {task.task_id} permanently failed")

    return None


# ---------------- WORKER LOOP ----------------
def worker_loop(scheduler):
    while True:
        task = scheduler.get_ready_task()
        if task:
            retry_task = execute_task(task)
            if retry_task:
                scheduler.schedule(retry_task)
        time.sleep(POLL_INTERVAL)


# ---------------- APPLICATION ENTRY ----------------
def main():
    scheduler = Scheduler()

    tasks = [
        Task(priority=1, execution_time=1),
        Task(priority=0, execution_time=3),
        Task(priority=2, execution_time=2, run_at=time.time() + 3),
    ]

    for task in tasks:
        scheduler.schedule(task)

    for _ in range(WORKER_COUNT):
        thread = threading.Thread(target=worker_loop, args=(scheduler,), daemon=True)
        thread.start()

    time.sleep(15)


if __name__ == "__main__":
    main()
