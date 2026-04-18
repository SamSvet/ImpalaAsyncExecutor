import time 
from uuid import uuid4
from typing import Optional, Callable, Tuple

from .interfaces import ConnectionBase, CursorBase


class ConnectionSync:
    def __init__(self, connection: ConnectionBase, cursor: CursorBase):
        self._id = str(uuid4)
        self.raw_connection = connection
        self.raw_cursor = cursor
        self.idle_since: Optional[float] = None

    @classmethod
    def create(
        cls
        ,connection_factory: Callable[[], Tuple[ConnectionBase, CursorBase]]
        ) -> "ConnectionSync":
        raw_conn, raw_curr = connection_factory()
        return cls(connection=raw_conn, cursor=raw_curr)
    
    @property
    def id(self) -> str:
        return self._id
    
    def mark_as_in_use(self) -> None:
        self.idle_since = None

    def mark_as_idle(self) -> None:
        self.idle_since = time.time()

    @property
    def idle_time(self) -> float:
        if self.idle_since is None:
            return 0.0
        return time.time() - self.idle_since
    
    def is_alive(self) -> bool:
        return self.raw_cursor.ping()
    
    def reset(self) -> None:
        #self.raw_connection.rollback()
        pass

    def close(self) -> None:
        try:
            if self.raw_cursor.ping():
                self.raw_cursor.close()
            self.raw_connection.close()
        except Exception:
            #This method must be fault-tolerant. A failure to close a connection,
            #which may already ve broken, should not disrupt Pool's cleanup operations.
            pass

