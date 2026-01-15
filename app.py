import time
import threading
from scheduler import Scheduler
from task import Task
from worker import execute_task
from config import WORKER_COUNT

scheduler = Scheduler()

def worker_loop():
    while True:
        task = scheduler.get_ready_task()
        if task:
            retry_task = execute_task(task)
            if retry_task:
                scheduler.schedule(retry_task)
        time.sleep(0.5)

def main():
    tasks = [
        Task(priority=1, execution_time=1),
        Task(priority=0, execution_time=3),
        Task(priority=2, execution_time=2, run_at=time.time() + 3)
    ]

    for task in tasks:
        scheduler.schedule(task)

    threads = []
    for _ in range(WORKER_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
        threads.append(t)

    time.sleep(15)

if __name__ == "__main__":
    main()
