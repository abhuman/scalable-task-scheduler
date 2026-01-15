import time
import uuid

class Task:
    def __init__(self, priority, execution_time, run_at=None):
        self.task_id = str(uuid.uuid4())
        self.priority = priority
        self.execution_time = execution_time
        self.run_at = run_at or time.time()
        self.retries = 0

    def __lt__(self, other):
        # Earlier run time first, then priority
        if self.run_at == other.run_at:
            return self.priority < other.priority
        return self.run_at < other.run_at
