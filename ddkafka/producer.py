from abc import ABC
from asyncio import Future
from functools import partial
from logging import getLogger
from typing import Any, ClassVar, Protocol

import msgpack
from aiokafka import AIOKafkaProducer
from ddutils.class_helpers import require_class_attribute

from ddkafka.config import ProducerConfig

logger = getLogger(__name__)


class Producible(Protocol):
    @property
    def idempotent_key(self) -> str | None: ...

    def model_dump(self, *args: Any, **kwargs: Any) -> dict: ...


def serialize_value(event: Producible) -> bytes:
    return msgpack.dumps(event.model_dump(mode='json'))


def serialize_key(key: Any) -> bytes | None:
    # aiokafka hands the key to the serializer even when it is None, and a key of b'None' would
    # route every keyless event to one partition.
    return None if key is None else str(key).encode('utf-8')


def _log_delivery_failure(topic: str, key: Any, future: Future) -> None:
    # `send` resolves when the record is buffered; the broker's answer lands in this future later.
    # Nobody awaits it, so without this callback a rejected record (`NotEnoughReplicas`, a timeout)
    # would only show up as "Future exception was never retrieved" from asyncio.
    if future.cancelled():
        return
    error = future.exception()
    if error is not None:
        logger.error({'message': 'KAFKA_PRODUCER: Delivery failed', 'topic': topic, 'key': key, 'error': repr(error)})


class BaseKafkaProducerRepository(ABC):
    """
    Base async Kafka producer.

    One `AIOKafkaProducer` is created lazily per subclass and shared by all its instances, so a
    producer repository can be instantiated freely. Events are serialized with msgpack from
    `model_dump(mode='json')` and keyed by `idempotent_key`.

    `create` and `bulk_create` return once the record is in the producer's buffer; aiokafka sends
    buffered records in batches (`linger_ms`, `max_batch_size`). A record the broker rejects is
    reported with an ERROR log entry, since nothing awaits its delivery.

    Class Attributes:
        bootstrap_servers: Kafka servers address
        config: Producer configuration

    Example:
        class EventProducerRepository(BaseKafkaProducerRepository):
            bootstrap_servers = ['kafka:9092']
            config = ProducerConfig(linger_ms=100)

        producer_repository_impl = EventProducerRepository()
        await producer_repository_impl.bulk_create('topic', events)
    """

    bootstrap_servers: ClassVar[list[str]]
    config: ClassVar[ProducerConfig] = ProducerConfig()

    _producer: ClassVar[AIOKafkaProducer | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        require_class_attribute(cls, 'bootstrap_servers', list)
        require_class_attribute(cls, 'config', ProducerConfig)

    @classmethod
    async def _get_producer(cls) -> AIOKafkaProducer:
        if cls._producer is None:
            cls._producer = AIOKafkaProducer(
                bootstrap_servers=cls.bootstrap_servers,
                value_serializer=serialize_value,
                key_serializer=serialize_key,
                **cls.config.model_dump(),
            )
            await cls._producer.start()

        return cls._producer

    @classmethod
    async def _send(cls, producer: AIOKafkaProducer, topic: str, event: Producible) -> None:
        key = event.idempotent_key
        future = await producer.send(topic, key=key, value=event)
        future.add_done_callback(partial(_log_delivery_failure, topic, key))

    async def create(self, topic: str, event: Producible) -> None:
        producer = await self._get_producer()
        await self._send(producer, topic, event)

    async def bulk_create(self, topic: str, events: list[Producible]) -> None:
        producer = await self._get_producer()
        for event in events:
            await self._send(producer, topic, event)


__all__ = ('BaseKafkaProducerRepository', 'Producible', 'serialize_key', 'serialize_value')
