import threading

class LamportClock:
    def __init__(self) -> None:
        self.__time = 0
        self.__lock = threading.Lock()

    def tick(self) -> int:
        with self.__lock:
            self.__time += 1
            return self.__time
    
    def update(self, t) -> int:
        with self.__lock:
            self.__time = max(t, self.__time) + 1
            return self.__time
    
    def value(self) -> int:
        with self.__lock:
            return self.__time