import time
from abc import ABC
from collections.abc import AsyncIterator, Iterator
from logging import getLogger
from typing import Any, ClassVar, Generic, Protocol, Self, TypeVar

import msgpack
from aiokafka import AIOKafkaConsumer, TopicPartition
from ddutils.class_helpers import get_generic_base_argument, require_class_attribute

from ddkafka.config import ConsumerConfig

logger = getLogger(__name__)


class Deserializable(Protocol):
    @classmethod
    def model_validate(cls, data: dict) -> Self: ...


DomainT = TypeVar('DomainT', bound=Deserializable)


class MessageDeserializationError(Exception):
    """A record that cannot be turned into the domain type; names the record so the offset can be skipped."""

    def __init__(self, topic: str, partition: int, offset: int):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        super().__init__(f'Cannot deserialize the message at {topic}[{partition}] offset {offset}')


class BaseKafkaConsumer(ABC, Generic[DomainT]):
    """
    Base async Kafka consumer: one instance reads one partition of `topic` in batches.

    Messages are msgpack payloads turned into the domain type with `model_validate`. `get_batches`
    polls until the partition runs dry or a poll comes back too thin (`min_batch_fill_ratio`),
    and `commit` acknowledges every batch yielded so far, including the one being processed.

    A record that cannot be deserialized (a schema the consumer does not know, a foreign payload)
    raises `MessageDeserializationError` with its offset, and the partition stays at that offset
    until someone moves it. A consumer whose stream can afford to lose such a record sets
    `skip_undeserializable = True`: the record is logged with its offset and dropped instead.

    Class Attributes:
        bootstrap_servers: Kafka servers address
        topic: Topic name
        group_id: Consumer group ID
        batch_size: Max records per poll
        poll_timeout_ms: Timeout for poll operation in milliseconds
        min_batch_fill_ratio: Minimum batch fill ratio to continue (0.0-1.0)
        skip_undeserializable: Drop records that cannot be deserialized instead of raising
        config: Consumer configuration

    Example:
        class ProfileEventConsumer(BaseKafkaConsumer[ProfileEvent]):
            bootstrap_servers = ['kafka:9092']
            topic = 'profile_events'
            group_id = 'analytics_context'
            batch_size = 100
    """

    bootstrap_servers: ClassVar[list[str]]
    topic: ClassVar[str]
    group_id: ClassVar[str]
    batch_size: ClassVar[int] = 1_000
    poll_timeout_ms: ClassVar[int] = 10_000
    min_batch_fill_ratio: ClassVar[float] = 0.1
    skip_undeserializable: ClassVar[bool] = False
    config: ClassVar[ConsumerConfig] = ConsumerConfig()

    _domain_class: ClassVar[type[Deserializable]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        domain_class = get_generic_base_argument(cls, BaseKafkaConsumer)
        if domain_class is None or isinstance(domain_class, TypeVar):
            raise TypeError(f'{cls.__name__} must specify domain type: class {cls.__name__}(BaseKafkaConsumer[YourDomain])')
        cls._domain_class = domain_class

        require_class_attribute(cls, 'bootstrap_servers', list)
        require_class_attribute(cls, 'topic', str)
        require_class_attribute(cls, 'group_id', str)
        if require_class_attribute(cls, 'batch_size', int) < 1:
            raise ValueError(f'`batch_size` class attribute of {cls.__name__} must be positive')
        if require_class_attribute(cls, 'poll_timeout_ms', int) < 0:
            raise ValueError(f'`poll_timeout_ms` class attribute of {cls.__name__} must not be negative')
        if not (0 <= require_class_attribute(cls, 'min_batch_fill_ratio', float) <= 1):
            raise ValueError(f'`min_batch_fill_ratio` class attribute of {cls.__name__} must be between 0 and 1')
        require_class_attribute(cls, 'skip_undeserializable', bool)
        require_class_attribute(cls, 'config', ConsumerConfig)

    def __init__(self, partition: int):
        self._partition = partition
        self._topic_partition = TopicPartition(self.topic, partition)
        self._processed_offsets: dict[TopicPartition, int] = {}
        self._create_consumer()

    def _deserialize(self, records: list[Any]) -> Iterator[DomainT]:
        # Done here rather than through aiokafka's `value_deserializer`, where a failure surfaces
        # from `getmany` without the record it happened on.
        for record in records:
            try:
                yield self._domain_class.model_validate(msgpack.loads(record.value))  # type: ignore[misc]
            except Exception as e:
                if not self.skip_undeserializable:
                    raise MessageDeserializationError(self.topic, self._partition, record.offset) from e
                logger.error(
                    {
                        'message': 'KAFKA_CONSUMER: Skipped a message that cannot be deserialized',
                        'topic': self.topic,
                        'partition': self._partition,
                        'offset': record.offset,
                        'error': repr(e),
                    }
                )

    def _create_consumer(self) -> None:
        logger.info(
            {
                'message': 'KAFKA_CONSUMER: Creating consumer',
                'topic': self.topic,
                'partition': self._partition,
                'group_id': self.group_id,
            }
        )

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            **self.config.model_dump(),
        )
        self._consumer.assign([self._topic_partition])

    async def start(self) -> None:
        await self._consumer.start()

        logger.info(
            {'message': 'KAFKA_CONSUMER: Started', 'topic': self.topic, 'partition': self._partition, 'group_id': self.group_id}
        )

    async def stop(self) -> None:
        await self._consumer.stop()
        logger.info(
            {'message': 'KAFKA_CONSUMER: Stopped', 'topic': self.topic, 'partition': self._partition, 'group_id': self.group_id}
        )

    async def _get_offset_limit(self, timestamp_threshold_minutes: int) -> int | None:
        """The first offset younger than the threshold, or None when the partition has no such message."""
        threshold_ms = int(time.time() * 1000) - (timestamp_threshold_minutes * 60 * 1000)
        offsets_for_times = await self._consumer.offsets_for_times({self._topic_partition: threshold_ms})
        offset_and_timestamp = offsets_for_times.get(self._topic_partition)
        return offset_and_timestamp.offset if offset_and_timestamp else None

    async def _poll(self, offset_limit: int | None) -> list[Any]:
        """One `getmany` for the partition; with `offset_limit` only the records before it."""
        records_by_partition = await self._consumer.getmany(
            self._topic_partition, timeout_ms=self.poll_timeout_ms, max_records=self.batch_size
        )
        records = records_by_partition.get(self._topic_partition, [])

        logger.info(
            {
                'message': 'KAFKA_CONSUMER: Retrieved batch',
                'topic': self.topic,
                'partition': self._partition,
                'records_count': len(records),
            }
        )

        if offset_limit is not None:
            records = [record for record in records if record.offset < offset_limit]
        return records

    async def get_batches(self, timestamp_threshold_minutes: int | None = None) -> AsyncIterator[tuple[DomainT, ...]]:
        """
        Returns an async iterator over batches of domain entities.

        Args:
            timestamp_threshold_minutes: Only messages OLDER than this threshold (in minutes) are included.
                                         Defaults to None (no filtering).
        """
        offset_limit: int | None = None
        if timestamp_threshold_minutes is not None:
            offset_limit = await self._get_offset_limit(timestamp_threshold_minutes)

        while True:
            records = await self._poll(offset_limit)
            if not records:
                break

            values = tuple(self._deserialize(records))
            records_count, last_offset = len(records), records[-1].offset
            # The raw records are not needed past this point; dropping them before the batch is
            # handed out frees the msgpack payloads while the caller works on the domain objects.
            del records

            # Recorded before the batch is handed out, so `commit` right after processing it
            # acknowledges this batch and not the previous one. The caller is responsible for not
            # committing a batch it failed on (the maker skips the commit on an exceptional exit).
            self._processed_offsets[self._topic_partition] = last_offset + 1

            if values:  # a batch of skipped records only is nothing to hand out
                yield values

            # A poll that came back mostly empty means the partition is nearly drained: finish
            # after this batch instead of waiting `poll_timeout_ms` for a handful more.
            if records_count < self.batch_size * self.min_batch_fill_ratio:
                break

    async def commit(self) -> None:
        if self._processed_offsets:
            await self._consumer.commit(self._processed_offsets)

        logger.info(
            {
                'message': 'KAFKA_CONSUMER: Committed offsets',
                'topic': self.topic,
                'partition': self._partition,
                'offsets': str(self._processed_offsets),
            }
        )

        self._processed_offsets.clear()


__all__ = ('BaseKafkaConsumer', 'Deserializable', 'MessageDeserializationError')
