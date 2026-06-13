import threading


class InventoryState:
    def __init__(self, initial=20):
        self._count = initial
        self._lock = threading.Lock()

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    def checkout(self) -> bool:
        with self._lock:
            if self._count > 0:
                self._count -= 1
                return True
            return False

    def restock(self, amount=5):
        with self._lock:
            self._count += amount

    def sync(self, new_count: int):
        with self._lock:
            self._count = new_count
