# ImpalaAsyncExecutor

`ImpalaAsyncExecutor` is a wrapper for running asynchronous Impala scripts that are independent of each other's execution sequence and results (e.g. batch of drop table operations).
It is not a client or driver for the Impala database and requires the pre-installed [impyla](https://github.com/cloudera/impyla) module. Supports python version starting from 3.6.

Key features of `ImpalaAsyncExecutor`:

- Asynchronous scripts are launched using the proven [asyncio](https://docs.python.org/3/library/asyncio.html) library within a single process and thread.
- Because of built-in connection pool (based on / inspired by [aiosqlitepool](https://github.com/slaily/aiosqlitepool/tree/main))there is no repeated opening and closing database connection.
- Only DML and DDL operations are supported. Running impala scripts does not involve reading data from the execution result.
- The launch configuration supports several conditions for returning the results of running SQL scripts. There is also control over the maximum number of queries simultaneously executed in the database.

### Dependencies

[impyla](https://github.com/cloudera/impyla) - client for HiveServer2 implementations such as Impala and Hive.

[jsonschema](https://python-jsonschema.readthedocs.io/en/stable/) - An implementation of the JSON Schema specification for Python

### Configuration

**`connection_factory`** - A function that creates and returns a tuple of a new HiveServer2Connection and HiveServer2Cursor instances. This function will be called whenever the pool needs to create a new connection.

**`config`** - Execution configuration dictionary. Configuration example [here](example/configuration.json).
It should satisfy the following json schema:

```python
{
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
```

**`max_async_requests_cnt`** (int) - Maximum number of requests executed simultaneously.

**`max_async_mem_limit`** (int) - Limit on the total amount of memory (mem_limit option) of simultaneously executed queries.

**`timeout`** (int) - Timeout in seconds for all asynchronous requests. After the timeout, all outstanding requests will be cancelled/closed.

**`return_when`** (str) - Conditions for returning the results of running SQL scripts. There are 4 modes of waiting for results(3 standard asyncio wait return_when options + 1 custom):

1. ALL_COMPLETED - Returns when all tasks are finished.
2. FIRST_COMPLETED - Returns as soon as any single task finishes or is cancelled.
3. FIRST_EXCEPTION - Returns when any task raises an exception, or when all tasks are completed if none fail.
4. FIRST_SUCCESSFUL - Custom option. Returns when any given task successfully finishes if any, or when all tasks are failed if none succeded.

**`requests`** (list) - A list of objects with individual parameters for a specific request. Each nested object contains the following settings:

- `file` - full path to the file containing the request text. For example: [first](example/first_operation.sql), [second](example/first_operation.sql), [third](example/third_operation.sql), [fourth](example/fourth_operation.sql).
- `configuration` - individual Impala query options (e.g. mem_limit, mt_dop, exec_time_limit_s). Only mem_limit is required, all other impala options are optional.
- `timeout` - timeout in seconds for each individual request.

### Usage

```python
import asyncio
import json
from impala.dbapi import connect
from impala_async_executor import ImpalaAsyncExecutor

def connection_factory():
    my_con = connect(host="MyHiveServer2.host.com", port=21050)
    my_cur = con.cursor()
    return my_con, my_cur

async_loop = asyncio.get_event_loop()
with open("[example/configuration.json](example/configuration.json)", "r") as json_config_file:
    config = json.load(json_config_file)
    impl_executor = ImpalaAsyncExecutor(config, connection_factory)
    results = async_loop.run_until_complete(impl_executor.execute())
    print(results)
```
