# KVDB

Учебная key-value база данных на Python со строковыми ключами и байтовыми
значениями. Данные хранятся в памяти, изменения сохраняются в журнал операций
(WAL), а полное состояние — в снимок на диске.

Реализованы лабораторные №1–№3: интерфейсы, локальный движок, Python API,
интерактивный CLI, HTTP REST-доступ и тесты. Несколько потоков и запросов
могут использовать один общий экземпляр движка; репликация и шардирование
относятся к лабораторным №4 и №5.

## Запуск

Требуются Python 3.10+ и Linux/POSIX. У движка и CLI нет внешних зависимостей;
HTTP-серверу нужны пакеты из `requirements.txt`. Выполняйте команды из корня
репозитория:

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

## HTTP API

Сервер третьей лабораторной открывает базу из каталога `--data-dir` на старте
и сохраняет данные при остановке. Значения передаются в JSON в кодировке
Base64. Установите зависимости и запустите сервер:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m kvdb.http --data-dir ./data --host 127.0.0.1 --port 8000
```

Примеры запросов (значение `b'hello world'` в Base64 — `aGVsbG8gd29ybGQ=`):

```bash
curl -X PUT localhost:8000/kv/hello -H 'Content-Type: application/json' \
     -d '{"value": "aGVsbG8gd29ybGQ="}'
curl localhost:8000/kv/hello
curl -X DELETE localhost:8000/kv/hello
curl -X POST localhost:8000/flush
curl localhost:8000/health
```

| Метод и путь | Назначение | Успех | Ошибки |
|---|---|---|---|
| `GET /health` | Доступность сервера | `200` | — |
| `GET /kv/{key}` | Значение ключа в Base64 | `200` | `404` — ключа нет |
| `PUT /kv/{key}` | Запись тела `{"value": "<base64>"}` | `200` | `400` — не Base64; `422` — неверное тело |
| `DELETE /kv/{key}` | Удаление ключа; отсутствие ключа — не ошибка | `200` | — |
| `POST /flush` | Снимок на диск и очистка WAL | `200` | — |

Ключи передаются в пути с URL-кодированием (`/kv/my%20key`), слэши внутри
ключа допустимы, пустой ключ адресуется как `/kv/`. Закрытая или заблокированная
после ошибки база отвечает `503`, ошибки дисковых операций — `500`.

Для постоянной работы на хосте подходит systemd-юнит:

```ini
[Unit]
Description=KVDB HTTP server
After=network.target

[Service]
WorkingDirectory=/opt/kvdb
ExecStart=/opt/kvdb/.venv/bin/python -m kvdb.http --data-dir /var/lib/kvdb --host 0.0.0.0 --port 8000
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

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
