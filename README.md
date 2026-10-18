# ddkafka

[![pypi](https://img.shields.io/pypi/v/ddkafka.svg)](https://pypi.python.org/pypi/ddkafka)
[![downloads](https://static.pepy.tech/badge/ddkafka/month)](https://pepy.tech/project/ddkafka)
[![versions](https://img.shields.io/pypi/pyversions/ddkafka.svg)](https://github.com/davyddd/ddkafka)
[![codecov](https://codecov.io/gh/davyddd/ddkafka/branch/main/graph/badge.svg)](https://app.codecov.io/github/davyddd/ddkafka)
[![license](https://img.shields.io/github/license/davyddd/ddkafka.svg)](https://github.com/davyddd/ddkafka/blob/main/LICENSE)

Kafka helpers: batch consumer, lock-guarded consumer maker and msgpack producer on top of aiokafka

## Installation

Install the library using pip:
```bash
pip install ddkafka
```

## Development

The project runs entirely in Docker. Requires Docker and [Fabric](https://www.fabfile.org/) on the host:

```bash
fab build      # build the dev image
fab tests      # run pytest
fab linters    # run ruff (with --fix), ty and complexipy
fab shell      # IPython inside the container
fab bash       # bash inside the container
```

This project was generated from [dd-lib-stub](https://github.com/davyddd/dd-lib-stub);
run `copier update` to pull in template updates.

## KafkaDsn

A settings field for the bootstrap servers of a cluster: `kafka://host[:port][,host[:port]]`, validated as a URL
like pydantic's `PostgresDsn`, with `bootstrap_servers` giving the `['host:port', ...]` list the aiokafka clients
take. A missing port is 9092.

```python
from pydantic_settings import BaseSettings

from ddkafka.dsn import KafkaDsn


class Settings(BaseSettings):
    KAFKA_URL: KafkaDsn  # KAFKA_URL=kafka://kafka-0:9092,kafka-1:9092


settings = Settings()
settings.KAFKA_URL.bootstrap_servers  # ['kafka-0:9092', 'kafka-1:9092']
```

## Producer

`BaseKafkaProducer` sends events serialized with msgpack from `model_dump(mode='json')`, keyed by
`idempotent_key`. The `AIOKafkaProducer` is obtained for every send from `producer_factory`, a
`ConnectionManagerFactory` from [ddutils](https://github.com/davyddd/ddutils) called with `alias`, the way a
[ddsql](https://github.com/davyddd/ddsql) adapter runs a query inside `factory(alias=...)`: the factory owns which
cluster the alias means, the scope the producer lives in (one `ScopedRegistry` per alias, a producer per process
and event loop), its lazy start with `create_producer` and its stop on shutdown.

```python
import os
from enum import Enum
from functools import partial

from aiokafka import AIOKafkaProducer
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry
from pydantic import BaseModel

from ddkafka.config import ProducerConfig
from ddkafka.producer import BaseKafkaProducer, create_producer


class ProfileEvent(BaseModel):
    profile_id: int
    action: str

    @property
    def idempotent_key(self) -> str | None:
        return f'{self.profile_id}:{self.action}'


class KafkaConnectionAlias(str, Enum):
    PRIMARY = 'primary'


producer_registries = {
    KafkaConnectionAlias.PRIMARY: ScopedRegistry[AIOKafkaProducer](
        create_func=partial(create_producer, settings.KAFKA_URL.bootstrap_servers, ProducerConfig(linger_ms=100)),
        scope_func=os.getpid,
        destructor_method_name='stop',
    ),
}
kafka_producer_factory = ConnectionManagerFactory(producer_registries, default=KafkaConnectionAlias.PRIMARY)


class EventProducer(BaseKafkaProducer):
    producer_factory = kafka_producer_factory
    # alias = KafkaConnectionAlias.PRIMARY  # None picks the factory's default


event_producer_impl = EventProducer()

await event_producer_impl.create('profile_events', ProfileEvent(profile_id=1, action='open_app'))
await event_producer_impl.bulk_create('profile_events', events)
```

Any object with `idempotent_key` and `model_dump` satisfies the `Producible` protocol; pydantic models only need the
property.

`ProducerConfig` defaults to `acks='all'`: a send is confirmed once every in-sync replica has the record, which is
the only mode where the cluster's `min.insync.replicas` applies. Replication factor and min ISR are cluster
settings chosen by devops; overriding `acks` in the application changes the delivery guarantee they set up, so
do it knowingly. Both configs are frozen pydantic models; every field carries a description with its trade-off.
Any other `aiokafka` keyword argument (`compression_type`, `max_poll_records`, ...) can be passed and is forwarded as is.

`create` and `bulk_create` return once the record is in the producer's buffer; aiokafka sends buffered records in
batches (`linger_ms`, `max_batch_size`), and a record the broker rejects is reported with an ERROR log entry. The
buffer only reaches Kafka while the process runs, so close the factory on shutdown (FastAPI lifespan, dramatiq
`after_worker_shutdown`): the registry's destructor is the producer's `stop`, which sends what is buffered.
Otherwise up to `linger_ms` worth of records is lost on SIGTERM:

```python
await kafka_producer_factory.close()
```

## Consumer

`BaseKafkaConsumer[Domain]` reads one partition in batches. Payloads are msgpack dictionaries turned into
`Domain` with `model_validate`. `get_batches()` polls until the partition runs dry or a poll comes back thinner than
`batch_size * min_batch_fill_ratio`; `commit()` acknowledges everything yielded so far.

```python
from ddkafka.consumer import BaseKafkaConsumer


class ProfileEventConsumer(BaseKafkaConsumer[ProfileEvent]):
    bootstrap_servers = ['kafka:9092']
    topic = 'profile_events'
    group_id = 'analytics_context'
    batch_size = 100


repo = ProfileEventConsumer(partition=0)
await repo.start()
try:
    async for batch in repo.get_batches():
        await process(batch)
        await repo.commit()
finally:
    await repo.stop()
```

`get_batches(timestamp_threshold_minutes=5)` yields only messages older than five minutes, for consumers that
deliberately lag behind the producer.

Tunables are class attributes: `batch_size`, `poll_timeout_ms`, `min_batch_fill_ratio` and `config`
(`ConsumerConfig`, passed to `AIOKafkaConsumer` as keyword arguments). Missing or mistyped attributes fail at
class definition.

A record that cannot be deserialized (a schema the consumer does not know, a payload written by something else)
raises `MessageDeserializationError` with the topic, partition and offset, and the partition stays at that offset
until the group's offset is moved past it. A consumer whose stream can afford to lose such a record sets
`skip_undeserializable = True`: the record is logged with its offset and dropped, the rest of the batch goes on.

## Consumer maker

`KafkaConsumerMaker` wraps a consumer in a distributed lock, so one partition is processed by one
worker at a time across the whole deployment. The lock is anything satisfying `Lockable` (`acquire`, `release`,
`extend`, `timeout`), for example `RedisLock` from [ddredis](https://github.com/davyddd/ddredis). Entering acquires
the lock and starts the consumer; every yielded batch is followed by a checkpoint (lock heartbeat + offset commit);
leaving commits, stops and releases. A lock that is already held raises from `acquire`, a consumer that fails to
start raises `ConsumerStartError` after releasing the lock.

```python
from ddredis.lock import RedisLock

from ddkafka.consumer_maker import KafkaConsumerMaker

async with KafkaConsumerMaker(
    consumer_class=ProfileEventConsumer,
    partition=0,
    lock_class=RedisLock,
    lock_kwargs={'client_factory': redis_client_factory, 'timeout': 300},
) as maker:
    async for batch in maker.get_batches():
        await process(batch)
```

Every batch is committed as a whole after the caller is done with it; a batch the caller fails on is not committed
and is read again next time, so processing must tolerate repeated records (`idempotent_key`). `batch_size` bounds
how much is reprocessed. `maker.checkpoint()` commits the current batch, so call it only once the batch is processed.

With `stream=True` the consumer does not return when the topic runs dry: it keeps the lock alive and polls
again after `stream_round_pause`, so a cron that starts it acts as a supervisor and messages are picked up as
they land. The pace must fit the lock: `stream_round_pause` plus the consumer's poll timeout may not exceed half
of the lock timeout, otherwise the maker refuses at construction.
