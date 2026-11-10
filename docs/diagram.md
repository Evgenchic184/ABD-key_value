# Контракты базы

Движок зависит от интерфейсов хранилища снимка и WAL. Их роль в системе
описана в [архитектуре](implementation.md#архитектура), а использование
публичных операций — в [контракте API](implementation.md#модель-данных-и-api).

```mermaid
classDiagram
  class KeyValueStore {
    <<interface>>
    +get(key: str) bytes | None
    +set(key: str, value: bytes) None
    +delete(key: str) None
    +flush() None
    +close() None
  }
  class SnapshotStorage {
    <<interface>>
    +load() dict~str, bytes~
    +save(data: Mapping~str, bytes~) None
  }
  class WAL {
    <<interface>>
    +append(record: bytes) None
    +replay() Iterator~bytes~
    +clear() None
  }
```

Роутер шардов не входит в контракт локального движка: он работает с узлами
по HTTP и не хранит данные. Его зависимости показаны отдельно.

```mermaid
classDiagram
  class ShardingConfig {
    +shards: tuple~ShardConfig~
    +virtual_nodes: int
    +timeout: float
    +reads: str
    +from_file(path) ShardingConfig
  }
  class ShardConfig {
    +id: str
    +leader: str
    +replicas: tuple~str~
    +read_urls: tuple~str~
  }
  class HashRing {
    +owner(key: str) str
    +token_hex(key: str) str
    +points() list~dict~
    +summary() dict
  }
  class ShardRouter {
    +get(key, consistency) ProxyResult
    +set(key, value) ProxyResult
    +delete(key) ProxyResult
    +flush() dict
    +locate(key) dict
    +cluster_status() dict
  }
  ShardingConfig "1" --> "*" ShardConfig
  ShardRouter --> HashRing
  ShardRouter --> ShardingConfig
```

Подробное описание и HTTP-контракт: [шардирование](sharding.md).
