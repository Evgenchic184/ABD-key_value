# Матрица тестов KVDB

Матрица описывает существующие автоматические проверки второй и третьей
лабораторных. Одна строка соответствует одной тестовой функции pytest.
Всего 61 функция, которая с учётом параметризации даёт **91 отдельную
проверку**. Каждый вариант выполняется с собственными фикстурами и временным
каталогом.

Команды установки зависимостей, запуска тестов и формирования HTML coverage
приведены в [README](../../README.md#тестирование).

## Движок и восстановление

Источник: [test_engine.py](../../tests/test_engine.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T01 | CRUD, замена, отсутствующий ключ, Unicode и пустые значения | Точные байты; отсутствующий ключ возвращает None | `test_crud` | 1 |
| T02 | Снимок и последующие set/delete в WAL | Повторное открытие восстанавливает итоговое состояние | `test_snapshot_and_wal_recovery` | 1 |
| T03 | Повторные flush/close и операции после закрытия | WAL пуст; close повторяем; остальные операции запрещены | `test_repeated_flush_and_close` | 1 |
| T04 | Исключение внутри with | Контекстный менеджер сохраняет состояние | `test_context_manager_saves_on_exception` | 1 |
| T05 | Неверные типы ключей и значений | TypeError; корректная запись после ошибки работает | `test_invalid_inputs_do_not_poison_database` | 4 |
| T06 | Ошибка сохранения снимка и повторный flush | WAL сохранён; восстановление и повторное сохранение успешны | `test_snapshot_failure_preserves_wal_and_allows_retry` | 1 |
| T07 | Ошибка fsync каталога снимка | WAL не очищается; данные восстанавливаются | `test_snapshot_directory_sync_failure_does_not_clear_wal` | 1 |
| T08 | Ошибка очистки WAL после снимка | Движок заблокирован; повторное применение WAL корректно | `test_clear_failure_leaves_replay_idempotent` | 1 |
| T09 | Ошибка после фактической очистки WAL | Данные восстанавливаются из снимка | `test_partial_clear_failure_recovers_from_snapshot` | 1 |
| T10 | Ошибка записи WAL | Память прежняя; операции заблокированы; close не сохраняет снимок | `test_wal_write_failure_leaves_memory_unchanged_and_blocks_database` | 1 |
| T11 | Ошибка WAL при delete | Значение в памяти сохраняется | `test_failed_delete_preserves_memory` | 1 |
| T12 | Запись WAL успешна, но вызывающий код получает ошибку | close не затирает журнал; новое значение восстанавливается | `test_uncertain_append_is_recovered_without_stale_snapshot` | 1 |
| T13 | KeyboardInterrupt после записи WAL | Журнал сохраняется и восстанавливается | `test_interrupted_append_preserves_journal_for_recovery` | 1 |
| T14 | Неверный JSON, структура, операция и Base64 в WAL | Открытие завершается явной ошибкой | `test_invalid_wal_operations_fail_open` | 10 |
| T15 | os._exit без close в дочернем процессе | Новый процесс восстанавливает запись, замену, удаление и бинарные данные | `test_crash_recovery_in_separate_process` | 1 |

## Снимки

Источник: [test_storage.py](../../tests/test_storage.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T16 | Снимок отсутствует или содержит пустую базу | Загружается пустой словарь | `test_missing_and_empty_snapshot` | 1 |
| T17 | Бинарные, Unicode и пустые данные; замена снимка | Точный round-trip; старые ключи исчезают после замены | `test_roundtrip_and_overwrite` | 1 |
| T18 | Вложенные каталоги | Каталоги создаются; снимок читается | `test_nested_directory` | 1 |
| T19 | Повреждённый JSON, версия, структура, UTF-8 и Base64 | Явная ошибка загрузки | `test_corrupt_formats` | 11 |
| T20 | Ошибка fsync файла или атомарной замены | Прежний снимок сохранён; временный файл удалён | `test_failures_before_replace_preserve_previous_snapshot` | 2 |
| T21 | Ошибка fsync каталога после замены | Ошибка сообщается; новый снимок уже может быть виден | `test_directory_sync_failure_is_reported` | 1 |
| T22 | Неверные типы данных при save | TypeError; прежний снимок не заменён | `test_invalid_types_do_not_replace_snapshot` | 2 |

## WAL и каталоги

Источник: [test_wal.py](../../tests/test_wal.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T23 | Порядок записей, пустой payload и бинарный формат | Длина, payload и CRC32 соответствуют формату; порядок сохранён | `test_order_empty_payload_and_wire_format` | 1 |
| T24 | Повторная очистка и запись после очистки | WAL обнуляется и остаётся пригодным для записи | `test_clear_and_reuse` | 1 |
| T25 | Незавершённые заголовок, payload или CRC; чрезмерная длина | Хвост обрезается; корректные записи сохранены; append работает | `test_incomplete_tail_is_trimmed_before_append` | 6 |
| T26 | Append без предварительного replay | Хвост восстанавливается до добавления новой записи | `test_append_without_explicit_replay_repairs_tail` | 1 |
| T27 | Повреждение CRC полной записи | Ошибка CRC32; файл не обрезается | `test_checksum_corruption_fails_without_truncation` | 1 |
| T28 | Ошибка fsync записи | Ошибка возвращается; новые append запрещены; полная запись может восстановиться | `test_append_waits_for_fsync` | 1 |
| T29 | Ошибка write и короткая запись | OSError; дальнейшие append заблокированы | `test_write_error_and_short_write_block_further_append` | 2 |
| T30 | Создание WAL | Синхронизируются файл и содержащий каталог | `test_creation_syncs_file_and_directory` | 1 |
| T31 | Ошибка синхронизации родителя после mkdir | Повторное открытие синхронизирует записи предков | `test_reopen_syncs_ancestor_after_directory_creation_failure` | 1 |
| T32 | Ошибка fsync каталога после создания WAL | Повторное открытие повторяет синхронизацию каталога | `test_reopen_retries_directory_sync_after_creation_failure` | 1 |
| T33 | Ошибка fsync очистки WAL | Ошибка возвращается; дальнейший append запрещён | `test_clear_sync_failure_blocks_wal` | 1 |

## CLI

Источник: [test_cli.py](../../tests/test_cli.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T34 | Команды, кавычки, пустое значение и перезапуски | Ожидаемый вывод, код 0 и сохранение изменений | `test_commands_and_restart` | 1 |
| T35 | Неверная команда, число аргументов, кавычки; help и EOF | Ошибки ввода не прерывают сеанс; EOF сохраняет данные | `test_input_errors_are_recoverable_and_eof_saves` | 1 |
| T36 | Повреждённый снимок | Ненулевой код и сообщение об ошибке хранения | `test_corrupt_snapshot_exits_nonzero` | 1 |
| T37 | Неверная операция в WAL | Ненулевой код и сообщение об ошибке хранения | `test_corrupt_wal_exits_nonzero` | 1 |
| T38 | Ctrl+C во время ожидания команды | Код 130; данные сохраняются | `test_keyboard_interrupt_closes_database` | 1 |
| T39 | Неопределённый результат записи WAL | Код 1; сохранённая операция восстанавливается | `test_storage_error_exits_nonzero_and_preserves_uncertain_wal` | 1 |
| T40 | Ошибка снимка при закрытии | Код 1 и сообщение об ошибке закрытия | `test_close_failure_exits_nonzero` | 1 |

## Конкурентный доступ

Источник: [test_concurrency.py](../../tests/test_concurrency.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T41 | Запись WAL приостановлена; параллельный get | Читатель получает старое значение до завершения записи | `test_read_does_not_wait_for_wal_append` | 1 |
| T42 | Первый set приостановлен после WAL; второй delete | Второй писатель ждёт; память совпадает с восстановленным состоянием | `test_writers_keep_wal_and_memory_in_same_order` | 1 |
| T43 | Flush во время незавершённого set | Flush ждёт; снимок содержит запись, WAL очищен | `test_flush_waits_for_pending_write` | 1 |
| T44 | Снимок приостановлен; параллельные get и set | Чтение доступно; set ждёт, затем восстанавливается из нового WAL | `test_read_during_snapshot_and_write_after_flush` | 1 |
| T45 | Close приостановлен; новый set | Set ждёт и после закрытия получает RuntimeError | `test_close_blocks_queued_write` | 1 |
| T46 | Неопределённая запись WAL; второй set в ожидании | Ошибка блокирует второй set и get; close сохраняет WAL для восстановления | `test_failed_write_blocks_queued_write_and_preserves_wal` | 1 |
| T47 | 4 писателя, читатель и flush: 320 изменений и 10 снимков | Ожидаемые значения и удаления; общий ключ совпадает с восстановленным состоянием | `test_concurrent_crud_and_flush_stress` | 1 |

## HTTP-доступ

Источник: [test_http.py](../../tests/test_http.py).

| ID | Сценарий | Ожидаемый результат | Автотест | Проверок |
|---|---|---|---|---:|
| T48 | Запрос health | Код 200 и статус ok | `test_health` | 1 |
| T49 | PUT, GET, замена, отсутствующий ключ и delete | Base64 round-trip; 404 для отсутствующего ключа | `test_crud_and_missing_key` | 1 |
| T50 | Бинарное значение всех 256 байтов | Точный round-trip через Base64 | `test_binary_values_roundtrip` | 1 |
| T51 | Пустой ключ и пустое значение | Ключ доступен как `/kv/`; значение — пустая строка Base64 | `test_empty_key_and_value` | 1 |
| T52 | Unicode-ключ с URL-кодированием | Декодированный ключ и точное значение | `test_unicode_key` | 1 |
| T53 | Ключ со слэшем | Конвертер path сохраняет полный ключ | `test_key_with_slash` | 1 |
| T54 | Delete отсутствующего ключа | 200; удаление идемпотентно | `test_delete_absent_key_is_idempotent` | 1 |
| T55 | Значение не в Base64 | 400; ключ не записан | `test_invalid_base64_is_rejected` | 1 |
| T56 | Неверное тело запроса | 422; ключ не записан | `test_invalid_body_is_rejected` | 1 |
| T57 | Flush после записи | Снимок на диске; WAL очищен | `test_flush_writes_snapshot_and_clears_wal` | 1 |
| T58 | Остановка и повторный запуск на том же каталоге | Записи и удаления восстановлены | `test_data_survives_restart` | 1 |
| T59 | RuntimeError движка | 503 с текстом ошибки | `test_closed_engine_returns_503` | 1 |
| T60 | OSError дисковой операции | 500 с текстом ошибки | `test_storage_error_returns_500` | 1 |
| T61 | Реальный запуск `python -m kvdb.http` в отдельном процессе | health и HTTP-запросы по сети; после SIGTERM снимок содержит запись | `test_uvicorn_entrypoint_serves_and_persists` | 1 |

## Границы проверок

- Проверяется общий экземпляр движка в одном процессе; границы поддержки
  описаны в [ограничениях реализации](../implementation.md#ограничения) и
  [границах синхронизации](../concurrency.md#сохранение-закрытие-и-границы-синхронизации).
- HTTP-тесты кроме T61 используют TestClient без реального сетевого обмена;
  T61 запускает uvicorn в отдельном процессе и обращается к нему по TCP.
- Есть небольшой стресс-тест корректности; нет измерений производительности и длительного стресс-теста.
- Аварийное завершение процесса проверяется; реальное отключение питания и сбои носителя не моделируются.
- Ошибки дисковых операций проверяются через mock; это не проверка конкретной файловой системы.
- Высокое покрытие строк и ветвей не доказывает отсутствие гонок.
