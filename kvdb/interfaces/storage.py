"""Интерфейс хранения снимка данных."""

from abc import ABC, abstractmethod
from collections.abc import Mapping


class SnapshotStorage(ABC):
    """Хранение состояния базы в снимке."""

    @abstractmethod
    def load(self) -> dict[str, bytes]:
        """Загрузить состояние."""

    @abstractmethod
    def save(self, data: Mapping[str, bytes]) -> None:
        """Сохранить состояние."""
