import time
from priority_queue import PriorityQueue

class Scheduler:
    def __init__(self):
        self.queue = PriorityQueue()

    def schedule(self, task):
        self.queue.push(task)

    def get_ready_task(self):
        task = self.queue.pop()
        if task and task.run_at <= time.time():
            return task
        if task:
            self.queue.push(task)
        return None
