"""Интерфейс key-value базы."""

from abc import ABC, abstractmethod


class KeyValueStore(ABC):
    """Операции над строковыми ключами и байтовыми значениями."""

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Получить значение по ключу."""

    @abstractmethod
    def set(self, key: str, value: bytes) -> None:
        """Записать значение."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Удалить ключ."""

    @abstractmethod
    def flush(self) -> None:
        """Сбросить изменения на диск."""

    @abstractmethod
    def close(self) -> None:
        """Закрыть базу."""
