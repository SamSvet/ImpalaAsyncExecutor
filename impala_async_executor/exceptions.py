class PoolClosedError(Exception):
    def __init__(self, message: str = "The connection pool has been closed unexpectedly."
    "This typically happens when either the pool.close() was called elsewhere in program or "
    "an unrecoverable error occured. Please check your application logs for errors that "
    "may have triggered this closure"):
        self.message = message
        super().__init__(self.message)


class PoolConnectionAcauireTimeoutError(Exception):
    def __init__(self, message: str = "Failed to acquire connection from pool within timeout period. "
    "This mayindicate that all connections are in use or the pool is under heavy load."):
        self.message = message
        super().__init__(self.message)