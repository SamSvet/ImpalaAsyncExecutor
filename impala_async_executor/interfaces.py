from abc import ABC, abstractmethod
from typing import Any


class ConnectionBase(ABC):
    @abstractmethod
    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass
    
    @abstractmethod
    def close(self) -> None:
        pass


class CursorBase(ABC):
    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def execute_async(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def is_executing(self) -> bool:
        pass

    @abstractmethod
    def status(self) -> str:
        pass

    @abstractmethod
    def ping(self) -> bool:
        pass

    @abstractmethod
    def get_profile(self) -> str:
        pass

    @abstractmethod
    def get_log(self) -> str:
        pass