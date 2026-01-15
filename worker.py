import time
from config import MAX_RETRIES

def execute_task(task):
    try:
        print(f"[Worker] Executing task {task.task_id}")
        time.sleep(task.execution_time)

        # Simulate occasional failure
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
