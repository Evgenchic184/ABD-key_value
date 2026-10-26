# KVDB

Учебная key-value база данных на Python со строковыми ключами и байтовыми
значениями. Данные хранятся в памяти, изменения сохраняются в журнал операций
(WAL), а полное состояние — в снимок на диске.

Реализованы лабораторные №1 и №2: интерфейсы, локальный движок, Python API,
интерактивный CLI и тесты. Несколько потоков могут использовать один общий
экземпляр движка. HTTP/RPC и развёртывание относятся к лабораторной №3,
репликация и шардирование — к №4 и №5.

## Запуск

Требуются Python 3.10+ и Linux/POSIX. У движка и CLI нет внешних зависимостей.
Выполняйте команды из корня репозитория:

```bash
python3 -m kvdb.cli.main --data-dir ./data
```

Каталог данных создаётся автоматически. Пример сеанса:

```text
kvdb> set "my key" "hello world"
OK
kvdb> get "my key"
b'hello world'
kvdb> exit
```

Команды: `set`, `get`, `delete`, `flush`, `exit`, `help`. Значения с пробелами
заключайте в кавычки; пустое значение — `""`. CLI принимает текст в UTF-8.
`exit`, EOF и Ctrl+C закрывают базу; после перезапуска данные восстанавливаются.

## Python API

```python
from pathlib import Path
from kvdb import FileStorage, FileWAL, KVEngine

path = Path("data")
with KVEngine(FileStorage(path / "snapshot.json"), FileWAL(path / "wal.bin")) as db:
    db.set("hello", b"world")
    assert db.get("hello") == b"world"
    db.delete("hello")
```

API принимает строковые ключи и байтовые значения. Контекстный менеджер
закрывает базу автоматически. [Контракт операций](docs/implementation.md#модель-данных-и-api)
и поведение при ошибках описаны в документации реализации.

## Тестирование

Создайте виртуальное окружение и установите зависимости для тестов:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest -v
```

Для запуска только стресс-теста добавьте `-k stress`.
Чтобы сохранить HTML-отчёт покрытия:

```bash
.venv/bin/python -m pytest --cov=kvdb --cov-report=term-missing --cov-report=html:reports/coverage
```

Отчёт находится в `reports/coverage/index.html`. Настройки pytest и coverage
хранятся в [pyproject.toml](pyproject.toml).

## Документация

- [Реализация](docs/implementation.md) — архитектура, запись, чтение и восстановление.
- [Интерфейсы](docs/diagram.md) — контракты компонентов.
- [Конкурентный доступ](docs/concurrency.md) — взаимодействие потоков и блокировок.
- [Матрица тестов](docs/testing/matrix.md) — сценарии и границы проверок.
- [Задания лабораторных](docs/labs/labs.md) — требования и этапы проекта.
