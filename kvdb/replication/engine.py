"""Cluster engine; keeps standalone formats and public contracts unchanged."""

import json
from threading import RLock

from kvdb.engine.engine import KVEngine
from kvdb.replication.models import Operation, ReadOnlyError, ReplicationConflict, SnapshotState


class ReplicatedEngine(KVEngine):
    def __init__(self, storage, wal, *, role: str, group_id: str):
        if role not in ("leader", "replica") or not group_id:
            raise ValueError("cluster engine requires role and group_id")
        self._storage, self._wal = storage, wal
        self._write_lock, self._state_lock = RLock(), RLock()
        self._closed = self._failed = False
        self.role, self.group_id = role, group_id
        if storage.path.exists():
            state = storage.load_state()
            if state.group_id != group_id:
                raise ValueError("snapshot belongs to another group")
            if role == "leader" and not state.initialized:
                raise ValueError("cannot start an uninitialized replica as leader")
        else:
            # Refuse to bind an old WAL to a new group / new snapshot.
            if wal.path.stat().st_size:
                raise ValueError("cluster requires a new directory or a v2 snapshot")
            state = SnapshotState(group_id, 0, role == "leader", {})
            storage.save_state(state)
        self._data = state.data.copy()
        self.last_seq, self.initialized = state.last_seq, state.initialized
        self.checkpoint_seq = state.last_seq
        previous = None
        for record in wal.replay():
            operation = Operation.parse(json.loads(record.decode("utf-8")))
            if previous is not None and operation.seq != previous + 1:
                raise ValueError("WAL sequence is not contiguous")
            previous = operation.seq
            if operation.seq <= self.last_seq:
                continue
            if not self.initialized or operation.seq != self.last_seq + 1:
                raise ValueError("WAL sequence gap")
            self._apply(operation)

    def _apply(self, operation: Operation):
        if operation.op == "set":
            self._data[operation.key] = operation.value
        else:
            self._data.pop(operation.key, None)
        self.last_seq = operation.seq

    def _ready(self):
        self._check()
        if not self.initialized:
            raise RuntimeError("Replica has not installed its first snapshot")

    def get(self, key):
        with self._state_lock:
            self._ready()
            self._validate_key(key)
            return self._data.get(key)

    def _mutate(self, op, key, value=None):
        with self._write_lock:
            with self._state_lock:
                self._check()
                if self.role != "leader":
                    raise ReadOnlyError("Replica is read-only")
                self._ready()
                self._validate_key(key)
                if op == "set" and not isinstance(value, bytes):
                    raise TypeError("Value must be bytes")
                operation = Operation(self.last_seq + 1, op, key, value)
            self._persist(operation)

    def _persist(self, operation):
        try:
            self._append(operation.wire())
            with self._state_lock:
                self._apply(operation)
        except BaseException:
            self._mark_failed()
            raise

    def set(self, key, value):
        self._mutate("set", key, value)

    def delete(self, key):
        self._mutate("delete", key)

    def apply_replication(self, raw):
        operation = Operation.parse(raw)
        with self._write_lock:
            with self._state_lock:
                self._ready()
                if self.role != "replica":
                    raise ValueError("only replicas apply remote operations")
                if operation.seq <= self.last_seq:
                    return
                if operation.seq != self.last_seq + 1:
                    raise ReplicationConflict("sequence_gap")
            self._persist(operation)

    def status(self):
        with self._state_lock:
            return {"role": self.role, "group_id": self.group_id,
                    "last_seq": self.last_seq, "checkpoint_seq": self.checkpoint_seq,
                    "ready": self.initialized and not self._failed and not self._closed}

    def _snapshot(self):
        return SnapshotState(self.group_id, self.last_seq, self.initialized, self._data.copy())

    def export_snapshot(self):
        with self._state_lock:
            self._ready()
            state = self._snapshot()
        return state.wire()

    def install_snapshot(self, raw):
        state = SnapshotState.parse(raw)
        with self._write_lock:
            with self._state_lock:
                self._check()
                if self.role != "replica" or state.group_id != self.group_id:
                    raise ValueError("snapshot group or role mismatch")
                if not state.initialized or state.last_seq < self.last_seq:
                    raise ValueError("snapshot is uninitialized or would roll back data")
            try:
                self._storage.save_state(state)
                self._wal.clear()
                with self._state_lock:
                    self._data = state.data.copy()
                    self.last_seq = self.checkpoint_seq = state.last_seq
                    self.initialized = True
            except BaseException:
                self._mark_failed()
                raise

    def changes(self, after: int, limit: int):
        with self._write_lock:
            with self._state_lock:
                self._ready()
                if after < self.checkpoint_seq:
                    raise ReplicationConflict("snapshot_required")
                if after > self.last_seq:
                    raise ReplicationConflict("position_ahead")
                result = self.status()
            operations = []
            for record in self._wal.replay():
                operation = Operation.parse(json.loads(record.decode("utf-8")))
                if operation.seq > after:
                    operations.append(operation.wire())
                    if len(operations) == limit:
                        break
            result["operations"] = operations
            return result

    def flush(self):
        with self._write_lock:
            with self._state_lock:
                self._check()
                state = self._snapshot()
            # A failure after replace/fsync has an ambiguous outcome: fail closed.
            try:
                self._storage.save_state(state)
                self._wal.clear()
                with self._state_lock:
                    self.checkpoint_seq = state.last_seq
            except BaseException:
                self._mark_failed()
                raise
