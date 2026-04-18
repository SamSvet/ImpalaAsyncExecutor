import asyncio
import time
from typing import Callable, Tuple, Set, List, Dict

import jsonschema

from .interfaces import ConnectionBase, CursorBase
from .client_sync import ImpalaConnectionPool, DataBaseProfile


class ImpalaAsyncExecutor():
    def __init__(self, config: Dict, connection_factory: Callable[[], Tuple[ConnectionBase, CursorBase]]):
        self.json_scema = {
            "type": "object",
            "properties": {
                "requests":{
                    "type":"array",
                    "minItems":1,
                    "items":{
                        "type":"object",
                        "properties":{
                            "file":{"type":"string"},
                            "configuration":{
                                "type":"object",
                                "patternProperties":{
                                    "^[a-zA-Z_][a-zA-Z0-9_]*$":{"type":"string"},
                                },
                                "additionalProperties": False,
                                "required": ["mem_limit"]
                            },
                            "timeout": {"type":"integer", "minimum":5, "maximum":3600}
                        },
                        "required":["file"]
                    }
                },
                "max_async_requests_cnt": {"type":"integer", "minimum":1, "maximum":10},
                "max_async_mem_limit": {"type":"integer", "minimum":1048576, "maximum": 53687091200},
                "timeout": {"type":"integer", "minimum":5, "maximum":3600},
                "return_when": {"enum":["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED", "FIRST_SUCCESSFUL"]}
            },
            "required":["requests", "max_async_mem_limit", "max_async_requests_cnt", "timeout", "return_when"]
        }
        jsonschema.validate(config, self.json_scema, format_checker=jsonschema.FormatChecker())
        self.config: Dict = config
        self._condition: asyncio.locks.Condition = asyncio.Condition()
        self._cur_mem_limit: float = 0.0
        self._cur_requests_cnt: int = 0
        self._connection_factory:Callable[[], Tuple[ConnectionBase, CursorBase]] = connection_factory


    @staticmethod
    def _read_file_content(file_path:str) -> str:
        with open(file_path, "r") as f:
            return f.read()
        
    @classmethod
    async def _execute_query(cls, cur: CursorBase, query_configuration: Dict, file_path: str):
        """
        Asynchronously execute SQL query.
        Every second polls status with HiveServer2Cursor `is_executing` and switches the context
        in case of incomplete requests.
        Args:
        cur: HS2Cursor object to request Impala.
        query_configuration: impala query configuration (e.g. mem_limit, mt_dop).
        file_path: SQL text file location
        """
        requests_str = cls._read_file_content(file_path)
        cur.execute_async(requests_str, configuration = query_configuration)
        while True:
            if not cur.is_executing():
                if cur.status == 'ERROR_STATE':
                    raise Exception(f"{cur.get_log()}")
                return
            await asyncio.sleep(1)

    @classmethod
    async def _close_pending_tasks(pending_tasks: Set[asyncio.Task]):
        """
        Async generator function to close pending tasks.
        In some cases asyncio.Task manages to complete before it is forced to close,
        then asyncio.Task result will be returned instead of a task exception info.
        Args:
        pending_tasks: Set of asyncio.Task instances to close.
        Returns:
        generator objectwith task exception info
        """
        for task in pending_tasks:
            task.cancel()
            try:
                result = await task
                yield result
            except asyncio.CancelledError:
                yield {'request_file': 'Unknown', 'rc':1, 'rs':'asyncio.CancelledError',
                       'query_info':{}}
            except Exception:
                if not task.cancelled():
                    yield task.exception.args[0]
                else:
                    yield {'request_file': 'Unknown', 'rc':1, 'rs':'Exception',
                           'query_info':{}}
                    
    async def _generate_results(self, initial_tasks: List[asyncio.Task]):
        """
        Async generator function combines self.config requests into `asyncio.wait` coroutine.
        In addition to standart 'ALL_COMPLETED', 'FIRST_EXCEPTION', 'FIRST_COMPLETED'
        supports 'FIRST_SUCCESSFUL' mode - returns when any given task successfully finishes if any.
        Args:
        initial_tasks: List of asyncio.Task's to submit in parallel
        Returns:
        generator object with tasks result info 
        """
        start_tm = time.perf_counter()
        config_rw = self.config["return_when"]
        return_when = "FIRST_COMPLETED" if config_rw == "FIRST_SUCCESSFUL" else config_rw
        config_timeout = self.config["timeout"]
        done, pending = await asyncio.wait(initial_tasks, return_when=return_when, timeout=config_timeout)
        while done:
            exit_flg=0
            for task in done:
                if not task.exception():
                    yield task.result()
                    exit_flg=1
                else:
                    yield task.exception.args[0]
            exit_flg = exit_flg if config_rw == "FIRST_SUCCESSFUL" else 1
            if exit_flg:
                break
            if pending:
                remain_tm = max(round(config_timeout - time.perf_counter() + start_tm), 0)
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED, timeout=remain_tm)
            else:
                break

        async for res in self._close_pending_tasks(pending):
            yield res

    
    async def _worker(self, id_num:int, connection_pool: ImpalaConnectionPool):
        """
        Coroutine executes async requests to impala.
        The number of tasks running in parallel is controlled by shared asyncio.Condition mutex.
        To submit SQL query on execution _worker should match 2 conditions:
        1) The total number of queries does not exceed 'max_async_requests_cnt' configuration value.
        2) The total amount of mem_limit option should not exceed 'max_async_mem_limit' configuration value.
        Args:
        id_num: config 'requests' array sequence number to operate.
        connection_pool: ImpalaConnectionPool instance - operates and establishes connections to Impala BD.
        Returns:
        A tuple of the file name and the corresponding execution result.
        """
        dbp = DataBaseProfile()
        cur, request_mem_limit, request_file = None, 0, ""
        try:
            request_file = self.config["requests"][id_num]["file"]
            query_configuration = self.config["requests"][id_num]["configuration"]
            query_timeout = self.config["requests"][id_num]["timeout"]
            max_mem_limit = self.config["max_async_mem_limit"]
            max_requests_cnt = self.config["max_async_requests_cnt"]
            request_mem_limit = min( int(query_configuration["mem_limit"]), max_mem_limit )
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._cur_mem_limit + request_mem_limit <= max_mem_limit 
                    and self._cur_requests_cnt < max_requests_cnt)
                self._cur_mem_limit += request_mem_limit
                self._cur_requests_cnt += 1
            with connection_pool.connection(dbp) as ctx:
                _, cur = ctx
                await asyncio.wait_for(self._execute_query(cur, query_configuration, request_file)
                                       , timeout=query_timeout)
            return {"request_file": request_file, "rc": 0, "rs": "Success", "query_info": dbp.info }
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception) as e:
            exc_type = 'CancelledError' if isinstance(e, asyncio.CancelledError) else (
                'TimeoutError' if isinstance(e, asyncio.TimeoutError) else f'{e}'
            )
            raise Exception({"request_file": request_file, "rc": 1, "rs": exc_type
                             , "query_info": dbp.info})
        finally:
            async with self._condition:
                self._cur_mem_limit -= request_mem_limit
                self._cur_requests_cnt -= 1
                self._condition.notify_all()


    async def execute(self):
        """
        Entry point for executing a batch of Impala asynchronous requests.
        Example:
        impl = ImpalaAsyncExecutor(json_dict, connection_factory)
        asyncio.run(impl.execute())
        Returns:
        List of a tasks results data
        """
        with ImpalaConnectionPool(self._connection_factory,
                                   self.config["max_async_requests_cnt"]) as pool:
            tasks = [asyncio.ensure_future(self._worker(i, pool)) for i, _ 
                     in enumerate(self.config["requests"])]
            results = [item async for item in self._generate_results(tasks)]
            return results