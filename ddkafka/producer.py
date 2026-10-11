from abc import ABC
from typing import Any, ClassVar, Protocol

import msgpack
from aiokafka import AIOKafkaProducer
from ddutils.class_helpers import require_class_attribute

from ddkafka.config import ProducerConfig


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


class BaseKafkaProducerRepository(ABC):
    """
    Base async Kafka producer.

    One `AIOKafkaProducer` is created lazily per subclass and shared by all its instances, so a
    producer repository can be instantiated freely. Events are serialized with msgpack from
    `model_dump(mode='json')` and keyed by `idempotent_key`.

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

    async def create(self, topic: str, event: Producible) -> None:
        producer = await self._get_producer()
        await producer.send(topic, key=event.idempotent_key, value=event)

    async def bulk_create(self, topic: str, events: list[Producible]) -> None:
        producer = await self._get_producer()
        for event in events:
            await producer.send(topic, key=event.idempotent_key, value=event)


__all__ = ('BaseKafkaProducerRepository', 'Producible', 'serialize_key', 'serialize_value')
