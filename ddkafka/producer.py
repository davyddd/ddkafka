from abc import ABC
from asyncio import Future
from collections.abc import Hashable
from functools import partial
from logging import getLogger
from typing import Any, ClassVar, Protocol

import msgpack
from aiokafka import AIOKafkaProducer
from ddutils.class_helpers import require_class_attribute
from ddutils.connections import ConnectionManagerFactory

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


async def create_producer(bootstrap_servers: list[str], config: ProducerConfig | None = None) -> AIOKafkaProducer:
    """
    A started `AIOKafkaProducer` with the library's serializers, for the `create_func` of the registry behind
    a producer factory (see `BaseKafkaProducer`).
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=serialize_value,
        key_serializer=serialize_key,
        **(config or ProducerConfig()).model_dump(),
    )
    await producer.start()
    return producer


class BaseKafkaProducer(ABC):
    """
    Base async Kafka producer.

    The `AIOKafkaProducer` is obtained for every send from `producer_factory`, a `ConnectionManagerFactory`
    called with `alias`, like a ddsql adapter runs a query inside `factory(alias=...)`. The factory owns the
    lifecycle: which cluster the alias means, the scope the producer lives in (one per process and event
    loop), its lazy start with `create_producer` and its stop on shutdown (`close()`). Events are serialized with msgpack from
    `model_dump(mode='json')` and keyed by `idempotent_key`.

    `create` and `bulk_create` return once the record is in the producer's buffer; aiokafka sends
    buffered records in batches (`linger_ms`, `max_batch_size`). A record the broker rejects is
    reported with an ERROR log entry, since nothing awaits its delivery. The buffer only reaches Kafka
    while the process runs: close the factory on application shutdown (the producer's `stop` sends what
    is buffered), otherwise up to `linger_ms` worth of records is lost on SIGTERM.

    Class Attributes:
        producer_factory: Opens a context handing out the producer of `alias`
        alias: The cluster to produce to; None picks the factory's default

    Example:
        producer_registries = {
            KafkaConnectionAlias.PRIMARY: ScopedRegistry[AIOKafkaProducer](
                create_func=partial(create_producer, ['kafka:9092'], ProducerConfig(linger_ms=100)),
                scope_func=process_loop_scope,
                destructor_method_name='stop',
            ),
        }
        kafka_producer_factory = ConnectionManagerFactory(producer_registries, default=KafkaConnectionAlias.PRIMARY)

        class EventProducer(BaseKafkaProducer):
            producer_factory = kafka_producer_factory

        event_producer_impl = EventProducer()
        await event_producer_impl.bulk_create('topic', events)
        ...
        await kafka_producer_factory.close()  # on shutdown
    """

    # The manager type is `Any`: a project's ConnectionManagerFactory[Alias, AIOKafkaProducer, ConnectionManager[...]]
    # must be assignable, and the type checker treats the parameter as invariant
    producer_factory: ClassVar[ConnectionManagerFactory[Any, AIOKafkaProducer, Any]]
    alias: ClassVar[Hashable | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        require_class_attribute(cls, 'producer_factory', ConnectionManagerFactory)

    @classmethod
    async def _send(cls, producer: AIOKafkaProducer, topic: str, event: Producible) -> None:
        key = event.idempotent_key
        future = await producer.send(topic, key=key, value=event)
        future.add_done_callback(partial(_log_delivery_failure, topic, key))

    async def create(self, topic: str, event: Producible) -> None:
        async with self.producer_factory(alias=self.alias) as producer:
            await self._send(producer, topic, event)

    async def bulk_create(self, topic: str, events: list[Producible]) -> None:
        async with self.producer_factory(alias=self.alias) as producer:
            for event in events:
                await self._send(producer, topic, event)


__all__ = ('BaseKafkaProducer', 'Producible', 'create_producer', 'serialize_key', 'serialize_value')
