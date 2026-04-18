from collections import deque

from typing import Optional, Callable, Tuple, Dict

from .connection_sync import ConnectionSync
from .interfaces import ConnectionBase, CursorBase
from .exceptions import PoolClosedError, PoolConnectionAcauireTimeoutError


class PoolSync:
    def __init__(self, 
                 connection_factory: Callable[[], Tuple[ConnectionBase, CursorBase]],
                 pool_size: Optional[int] = 5,
                 acquisition_timeout: Optional[int] = 30,
                 idle_timeout: Optional[int] = 86400,
                 operation_timeout: Optional[int] = 10):
        """
        Initializes a new connection pool.
        Args:
            connection_factory: A sync callable that returns a new Connection, Cursor tuple.
            pool_size: The maximum number of connections to keep in the pool. Defaults to 5.
            acquisition_timeout: The maximum number of seconds to wait for a connection to become
                available before raising a timeout error. Defaults to 30. 
            idle_timeout: The maximum number of seconds that a connection can remain idle in the pool
                before being closed and replaced. This helps prevent issues with firewalls or database
                servers closing stale connections. Defaults to 86400.
            operation_timeout: The maximum number of seconds to wait for a connection operation 
                (e.g. reset, close) to complete. Default to 10.
        """
        self._connection_factory = connection_factory
        self._pool_size = pool_size
        self._acquisition_timeout = acquisition_timeout
        self._idle_timeout = idle_timeout
        self._operation_timeout = operation_timeout
        self._idle_queue: deque[ConnectionSync] = deque(maxlen=pool_size)
        self._connection_registry: Dict[str, ConnectionSync] = {}
        self._closed_flg = False

    @property
    def is_closed(self) -> bool:
        return self._closed_flg

    @property
    def size(self) -> int:
        return len(self._idle_queue)


    def _retire_connection(self, conn: ConnectionSync):
        """Close a connection and remove it from the registry."""
        conn.close()
        self._connection_registry.pop(conn.id, None)
    
    def release(self, conn: ConnectionSync):
        if self.is_closed:
            if conn:
                self._retire_connection(conn)
            return
        
        if conn.id not in self._connection_registry:
            return
        
        conn.mark_as_idle()
        if len(self._connection_registry) > self._pool_size:
            self._connection_registry.pop(conn.id, None)
            conn.close()
            return
        
        self._idle_queue.append(conn)

    
    def close(self):
        if self.is_closed:
            return
        
        self._closed_flg = True
        self._idle_queue.clear()
        
        for conn in self._connection_registry.values():
            conn.close()
        self._connection_registry.clear()

    
    def _claim_if_healthy(self, conn: ConnectionSync) -> bool:
        if conn.idle_time > self._idle_timeout:
            return False
        
        if conn.is_alive():
            conn.mark_as_in_use()
            return True
        return False
    

    def _cleanup_broken_connections(self):
        for conn in self._connection_registry.values():
            if not conn.is_alive():
                self._retire_connection(conn)

    
    def _try_provision_new_connection(self) -> Optional[ConnectionSync]:
        if len(self._connection_registry) >= self._pool_size:
            self._cleanup_broken_connections()
            if len(self._connection_registry) >= self._pool_size:
                return
            
        new_conn = None
        try:
            new_conn = ConnectionSync.create(self._connection_registry)
            if not new_conn.is_alive() and new_conn:
                new_conn.close()
                return
            self._connection_registry[new_conn.id] = new_conn
            new_conn.mark_as_in_use()
            return new_conn
        except Exception:
            if new_conn:
                new_conn.close()
    
    def _wait_for_healthy_connection(self) -> ConnectionSync:
        if self.is_closed:
            raise PoolClosedError()
        
        while self._idle_queue:
            conn = self._idle_queue.popleft()
            if self._claim_if_healthy(conn):
                return conn
            self._retire_connection(conn)

    def _run_acquisition_cycle(self) -> Optional[ConnectionSync]:
        if self.is_closed:
            raise PoolClosedError()
        
        while self._idle_queue:
            conn = self._idle_queue.popleft()
            if self._claim_if_healthy(conn):
                return conn
            self._retire_connection(conn)

        conn = self._try_provision_new_connection()
        if conn:
            return conn
        raise PoolConnectionAcauireTimeoutError()
        #return self._wait_for_healthy_connection()

    def acquire(self) -> ConnectionSync:
        return self._run_acquisition_cycle()

