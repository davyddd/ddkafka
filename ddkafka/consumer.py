import time
from abc import ABC
from collections.abc import AsyncIterator
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


class BaseKafkaConsumerRepository(ABC, Generic[DomainT]):
    """
    Base async Kafka consumer repository: one instance reads one partition of `topic` in batches.

    Messages are msgpack payloads turned into the domain type with `model_validate`. `get_batches`
    polls until the partition runs dry or a poll comes back too thin (`min_batch_fill_ratio`),
    and `commit` acknowledges everything yielded so far.

    Class Attributes:
        bootstrap_servers: Kafka servers address
        topic: Topic name
        group_id: Consumer group ID
        batch_size: Max records per poll
        poll_timeout_ms: Timeout for poll operation in milliseconds
        min_batch_fill_ratio: Minimum batch fill ratio to continue (0.0-1.0)
        config: Consumer configuration

    Example:
        class ProfileEventConsumerRepository(BaseKafkaConsumerRepository[ProfileEvent]):
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
    config: ClassVar[ConsumerConfig] = ConsumerConfig()

    _domain_class: ClassVar[type[Deserializable]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        domain_class = get_generic_base_argument(cls, BaseKafkaConsumerRepository)
        if domain_class is None or isinstance(domain_class, TypeVar):
            raise TypeError(
                f'{cls.__name__} must specify domain type: class {cls.__name__}(BaseKafkaConsumerRepository[YourDomain])'
            )
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
        require_class_attribute(cls, 'config', ConsumerConfig)

    def __init__(self, partition: int):
        self._partition = partition
        self._topic_partition = TopicPartition(self.topic, partition)
        self._processed_offsets: dict[TopicPartition, int] = {}
        self._create_consumer()

    def _deserialize_value(self, value: bytes) -> DomainT:
        return self._domain_class.model_validate(msgpack.loads(value))  # type: ignore[return-value]

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
            value_deserializer=self._deserialize_value,
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

            if not records:
                break

            if offset_limit is not None:
                records = [record for record in records if record.offset < offset_limit]
                if not records:
                    break

            # A poll that came back mostly empty means the partition is nearly drained: finish
            # after this batch instead of waiting `poll_timeout_ms` for a handful more.
            should_break = len(records) < self.batch_size * self.min_batch_fill_ratio

            yield tuple(record.value for record in records)

            self._processed_offsets[self._topic_partition] = records[-1].offset + 1

            if should_break:
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


__all__ = ('BaseKafkaConsumerRepository', 'Deserializable')
