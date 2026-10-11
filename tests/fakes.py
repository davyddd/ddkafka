"""Stand-ins for aiokafka and the lock, so the tests run without a broker."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import msgpack
from aiokafka import TopicPartition
from pydantic import BaseModel

if TYPE_CHECKING:
    from aiokafka.structs import OffsetAndTimestamp

BOOTSTRAP_SERVERS = ['kafka:9092']


class Event(BaseModel):
    event_id: int
    name: str

    @property
    def idempotent_key(self) -> str | None:
        return str(self.event_id)


@dataclass
class Record:
    offset: int
    value: Any


class FakeAIOKafkaConsumer:
    """Returns the prepared polls one by one, then empty ones; records go through the deserializer."""

    def __init__(self, *, value_deserializer, **kwargs: Any):
        self.kwargs = kwargs
        self.value_deserializer = value_deserializer
        self.polls: list[list[Record]] = []
        self.offsets_for_times_result: dict[TopicPartition, OffsetAndTimestamp | None] = {}
        self.assigned: list[TopicPartition] = []
        self.committed: list[dict[TopicPartition, int]] = []
        self.started = False
        self.stopped = False
        self.getmany_calls: list[dict[str, Any]] = []

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = partitions

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, *partitions: TopicPartition, timeout_ms: int, max_records: int) -> dict:
        self.getmany_calls.append({'timeout_ms': timeout_ms, 'max_records': max_records})
        if not self.polls:
            return {}
        records = [Record(record.offset, self.value_deserializer(record.value)) for record in self.polls.pop(0)]
        return {partitions[0]: records}

    async def offsets_for_times(self, _timestamps: dict[TopicPartition, int]) -> dict:
        return self.offsets_for_times_result

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        self.committed.append(dict(offsets))


def packed(event_id: int, name: str = 'x') -> bytes:
    return msgpack.dumps(Event(event_id=event_id, name=name).model_dump(mode='json'))


class FakeAIOKafkaProducer:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.started = False
        self.sent: list[tuple[str, Any, Any]] = []

    async def start(self) -> None:
        self.started = True

    async def send(self, topic: str, key: Any = None, value: Any = None) -> None:
        self.sent.append((topic, self.kwargs['key_serializer'](key), self.kwargs['value_serializer'](value)))


@dataclass
class FakeLock:
    key: str
    timeout: float = 60.0
    fail_acquire: bool = False
    calls: list[str] = field(default_factory=list)

    async def acquire(self) -> None:
        if self.fail_acquire:
            raise RuntimeError('lock is taken')
        self.calls.append('acquire')

    async def release(self) -> None:
        self.calls.append('release')

    async def extend(self) -> None:
        self.calls.append('extend')
