import heapq
import threading

class PriorityQueue:
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

    def is_empty(self):
        with self.lock:
            return len(self.heap) == 0
