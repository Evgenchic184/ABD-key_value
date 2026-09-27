from abc import ABC, abstractmethod
from typing import Optional


class KeyValueStore(ABC):
    """Интерфейс key-value хранилища."""

    @abstractmethod
    def get(self, key: str) -> Optional[bytes]:
        """Вернуть значение по ключу или None, если ключа нет."""
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: bytes) -> None:
        """Записать значение по ключу."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        """Удалить ключ."""
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        """Сбросить данные на диск."""
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Закрыть хранилище."""
        raise NotImplementedError
