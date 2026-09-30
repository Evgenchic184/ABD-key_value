"""Интерфейс журнала записей."""

from abc import ABC, abstractmethod
from collections.abc import Iterator


class WAL(ABC):
    """Журнал байтовых записей."""

    @abstractmethod
    def append(self, record: bytes) -> None:
        """Добавить запись."""

    @abstractmethod
    def replay(self) -> Iterator[bytes]:
        """Прочитать записи."""

    @abstractmethod
    def clear(self) -> None:
        """Очистить журнал."""
