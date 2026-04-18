from typing import Callable, Optional ,Tuple, Dict
from contextlib import contextmanager
import re

from .pool_sync import PoolSync
from .interfaces import ConnectionBase, CursorBase


class DataBaseProfile():
    def __init__(self):
        self.info: Dict = {}


    def save_cursor_info(self, curr: CursorBase):
        """
        Return a dict with the following keys:
        'Query ID', 'Session ID', 'Start Time', 'End Time', 'Impala Query State'.
        Key values are taken from HiveServer2Cursor `get_profile`.
        Suppresses all exceptions, which causes an empty dict to be returned.

        :param curr: impala.hiveserver2.HiveServer2Cursor whose profile is used to extract an information.
        """
        try:
            log_list = curr.get_profile().split('\n')[:20]
            match = re.search(r"Query \(id=(?P<query_id>[\da-f]+:[\da-f]+)\):", log_list[0])
            query_id = match.group('query_id') if match else 'Unknown'
            subs = ['Session ID', 'Start Time', 'End Time', 'Impala Query State']
            result_set = [s for s in log_list if any(item in s for item in subs)]
            result_dict = {'Query ID': query_id}
            for items in result_set:
                k, v = items.split(":", 1)
                k, v = k.strip(), v.strip()
                result_dict[k] = v
            self.info = result_dict
        except:
            pass


class ImpalaConnectionPool():
    def __init__(self,
                    connection_factory: Callable[[], Tuple[ConnectionBase, CursorBase]],
                    pool_size: Optional[int] = 5,
                    acquisition_timeout: Optional[int] = 30,
                    idle_timeout: Optional[int] = 86400,
                    operation_timeout: Optional[int] = 10):
        """
        Initialises the high-level connection pool manager.
        Args:
        connection_factory: An async callable that returns a new raw database connection.
        pool_size: The maximum number of connections to keep in the pool. Defaults to 5.
        acquisition_timeout: The maximum number of seconds to wait for a connection to become
            available before raising a timeout error. Defaults to 30. 
        idle_timeout: The maximum number of seconds that a connection can remain idle in the pool
            before being closed and replaced. This helps prevent issues with firewalls or database
            servers closing stale connections. Defaults to 86400.
        operation_timeout: The maximum number of seconds to wait for a connection operation 
            (e.g. reset, close) to complete. Default to 10.
        """
        self._pool = PoolSync(
            connection_factory=connection_factory,
            pool_size=pool_size,
            acquisition_timeout=int(acquisition_timeout),
            idle_timeout=int(idle_timeout),
            operation_timeout=int(operation_timeout)
        )

    @contextmanager
    def connection(self, dbp: DataBaseProfile = DataBaseProfile()):
        conn = self._pool.acquire()
        try:
            yield conn.raw_connection, conn.raw_cursor
        finally:
            dbp.save_cursor_info(conn.raw_cursor)
            self._pool.release(conn)
    
    def acquire(self):
        return self._pool.acquire()
    
    def close(self):
        self._pool.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self.close()