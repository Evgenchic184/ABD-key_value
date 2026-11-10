# KVDB

Учебная key-value база данных на Python со строковыми ключами и байтовыми
значениями. Данные хранятся в памяти, изменения сохраняются в журнал операций
(WAL), а полное состояние — в снимок на диске.

База предоставляет Python API, интерактивный CLI, HTTP REST-доступ,
асинхронную репликацию через WAL и прокси-шардирование по consistent hashing.
Есть Docker-стенды с ведущим и репликами, шардированным кластером,
веб-интерфейс и демонстрационная нагрузка.

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

HTTP-сервер открывает базу из каталога `--data-dir` на старте
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

## Репликация и демонстрационный стенд

```bash
mkdir -p reports/demo
docker compose up --build -d
```

Откройте **http://localhost:8080**. Нажмите «Запустить нагрузку»: три пользователя параллельно
обновляют счётчики `user-1`–`user-3` и удаляют временные ключи. Панель показывает позиции WAL, запросы,
отставание реплик и контрольное чтение. Дождитесь начальной синхронизации,
затем останавливайте и возвращайте узлы:

```bash
docker compose stop replica1
docker compose start replica1
docker compose stop leader
docker compose start leader
```

При остановке ведущего изменения недоступны, чтения продолжаются с реплик.
Кнопка «Проверить сходимость» приостанавливает нагрузку и сравнивает позиции
и данные узлов. Демонстрация работает без pytest. JSONL-журналы сохраняются
в `reports/demo/`; у каждого узла свой постоянный Docker volume.

Узлы доступны на портах 8000–8002. Standalone остаётся режимом по умолчанию;
кластерные роли — `--role leader|replica`, группа — `--group-id`, адрес ведущего
для реплики — `--leader-url`. Кластер требует новых каталогов данных.
Кнопки на карточках реплик приостанавливают синхронизацию, сохраняя чтение.
Таблица счётчиков и отставание WAL показывают накопившуюся разницу; после
возобновления реплика догоняет ведущего. Форма «Запрос к базе» позволяет
выполнять PUT/GET/DELETE вручную, значения вводятся и выводятся как UTF-8.
Веб-интерфейс — обычный сервис `web`, профиль для его запуска не требуется.
Локально он запускается командой `python -m kvdb.web`.
Полный [протокол, схемы, команды и сценарий отказов](docs/replication.md).

```bash
# Остановить стенд, сохранив данные
docker compose down
```

## Шардирование

Роутер `kvdb.sharding` принимает тот же REST API, что и обычный узел, и
распределяет ключи между независимыми группами репликации. Шард выбирается
по кольцу consistent hashing с виртуальными узлами; записи идут ведущему
шарда, чтения — репликам с переходом на ведущего. Роутер не хранит данные:
он проксирует запросы и показывает топологию, позиции узлов и статистику.

```bash
docker compose -f compose.sharding.yaml up --build -d
curl localhost:8000/health
curl localhost:8000/sharding/status
curl -X PUT localhost:8000/kv/hello -H 'Content-Type: application/json' \
     -d '{"value": "d29ybGQ="}'
curl localhost:8000/sharding/locate/hello
curl 'localhost:8000/kv/hello?consistency=leader'
```

Стенд содержит два шарда, у каждого свой ведущий и реплика. Порты роутера —
8000, шардов — 8100/8101 и 8200/8201. Для строгого чтения после записи
используйте `?consistency=leader`; по умолчанию чтение может отставать от
ведущего, как и у обычной реплики. Запуск без Docker, JSON-конфигурация,
сценарий отказов и ограничения описаны в
[документе о шардировании](docs/sharding.md).

```bash
# Остановить шардированный стенд, сохранив данные
docker compose -f compose.sharding.yaml down
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
- [Репликация и демонстрация](docs/replication.md) — протокол WAL, восстановление, Docker и сценарий отказов.
- [Шардирование](docs/sharding.md) — consistent hashing, роутер, конфигурация и сценарий отказов.
- [Конкурентный доступ](docs/concurrency.md) — взаимодействие потоков и блокировок.
- [Матрица тестов](docs/testing/matrix.md) — сценарии и границы проверок.
- [Задания лабораторных](docs/labs/labs.md) — требования и этапы проекта.
