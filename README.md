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
run `uvx copier update` to pull in template updates.

## Compatibility

Python 3.11 to 3.14. What changed in each release, breaking changes included,
is in the [GitHub releases](https://github.com/davyddd/ddkafka/releases).

<!-- The sections below are the library's own; copier update leaves them alone -->

Depends on `aiokafka>=0.12,<0.15`, `msgpack>=1,<2`, `ddutils>=0.4.0,<0.5.0` and `pydantic>=2.10,<3` (see `KafkaDsn`).
The tests run against fakes of the aiokafka clients, no broker is needed.

## KafkaDsn

A settings field for the bootstrap servers of a cluster: `kafka://host[:port][,host[:port]]`, validated as a URL
like pydantic's `PostgresDsn`, with `bootstrap_servers` giving the `['host:port', ...]` list the aiokafka clients
take. A missing port is 9092.

`pydantic>=2.10` comes from here: `KafkaDsn` subclasses `pydantic.networks._BaseMultiHostUrl` the way `PostgresDsn`
does. It is not a public pydantic API, so a pydantic upgrade is where to look if the DSN breaks.

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
from enum import Enum
from functools import partial

from aiokafka import AIOKafkaProducer
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry, process_loop_scope
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
        create_func=partial(create_producer, ['kafka-0:9092', 'kafka-1:9092'], ProducerConfig(linger_ms=100)),
        scope_func=process_loop_scope,
        destructor_method_name='stop',
    ),
}
kafka_producer_factory = ConnectionManagerFactory(producer_registries, default=KafkaConnectionAlias.PRIMARY)


class EventProducer(BaseKafkaProducer):
    producer_factory = kafka_producer_factory
    # alias = KafkaConnectionAlias.PRIMARY  # None picks the factory's default


event_producer_impl = EventProducer()

await event_producer_impl.create('profile_events', ProfileEvent(profile_id=1, action='open_app'))
await event_producer_impl.bulk_create('profile_events', [ProfileEvent(profile_id=2, action='purchase')])
```

Any object with `idempotent_key` and `model_dump` satisfies the `Producible` protocol; pydantic models only need the
property. The key becomes the record key as `str(key).encode()`, so records with the same key land on the same
partition; a key of `None` sends no key and the record is spread round-robin over the partitions. The value is
`msgpack.dumps(event.model_dump(mode='json'))`, so UUIDs and datetimes travel as strings. The pieces are public for
a producer wired by hand: `create_producer(bootstrap_servers, config=None)` returns a started `AIOKafkaProducer` with
the library's `serialize_key` and `serialize_value`.

`ProducerConfig` is a frozen pydantic model passed to `AIOKafkaProducer` as keyword arguments; every field carries a
description with its trade-off, and any other aiokafka argument (`compression_type`, `max_request_size`, ...) is
forwarded as is.

| Field | Default | |
|---|---|---|
| `acks` | `'all'` | `0`, `1` or `'all'`; see below |
| `enable_idempotence` | `False` | requires `acks='all'`, the config refuses the combination otherwise |
| `linger_ms` | `50` | how long records wait to share a batch; `0` sends each at once |
| `max_batch_size` | `32_000` | bytes per partition batch |
| `retry_backoff_ms` | `500` | pause before a retried request |
| `metadata_max_age_ms` | `30_000` | metadata refresh without errors |
| `request_timeout_ms` | `5_000` | keep above the broker's `replica.lag.time.max.ms` with `acks='all'` |

`acks='all'` confirms a send once every in-sync replica has the record, which is the only mode where the cluster's
`min.insync.replicas` applies. Replication factor and min ISR are cluster settings chosen by devops; overriding
`acks` in the application changes the delivery guarantee they set up, so do it knowingly. The flip side: a topic
with fewer replicas than `min.insync.replicas` rejects every record with `NotEnoughReplicas`. `create()` does not
fail on it, the rejection arrives later and is only visible as the `KAFKA_PRODUCER: Delivery failed` ERROR log
entry. A single-broker compose works because its `min.insync.replicas` is 1; a cluster whose topics are not yet
configured for `acks='all'` needs `ProducerConfig(acks=1)` until they are.

`create` and `bulk_create` return once the record is in the producer's buffer; aiokafka sends buffered records in
batches (`linger_ms`, `max_batch_size`), and a record the broker rejects is reported with an ERROR log entry, since
nothing awaits its delivery. The buffer only reaches Kafka while the process runs, so close the factory on shutdown
(FastAPI lifespan, dramatiq `after_worker_shutdown`): the registry's destructor is the producer's `stop`, which
sends what is buffered. Otherwise up to `linger_ms` worth of records is lost on SIGTERM:

```python
await kafka_producer_factory.close()
```

## Consumer

`BaseKafkaConsumer[Domain]` reads one partition in batches: one instance is assigned one partition of `topic`
(`assign`, not `subscribe`), so it never joins the consumer group and the group only stores its offsets. Payloads are
msgpack dictionaries turned into `Domain` with `model_validate` (the `Deserializable` protocol, pydantic models have
it). `get_batches()` polls until the partition runs dry or a poll comes back thinner than
`batch_size * min_batch_fill_ratio`; `commit()` acknowledges every batch yielded so far.

```python
from ddkafka.consumer import BaseKafkaConsumer


class ProfileEventConsumer(BaseKafkaConsumer[ProfileEvent]):
    bootstrap_servers = ['kafka-0:9092', 'kafka-1:9092']
    topic = 'profile_events'
    group_id = 'analytics_context'
    batch_size = 100


consumer = ProfileEventConsumer(partition=0)  # needs a running event loop: aiokafka binds the client to it
await consumer.start()
try:
    async for batch in consumer.get_batches():
        await process(batch)   # your handler
        await consumer.commit()
finally:
    await consumer.stop()
```

The offsets of a batch are recorded before it is handed out, so `commit()` right after processing it acknowledges
that batch and not the previous one. Call it after the batch is processed: a `commit()` before means a failure in
the middle loses those records. The caller is responsible for not committing a batch it failed on.

The class attributes, checked at class definition (`AttributeError` when missing, `TypeError` when mistyped,
`ValueError` when out of range):

| Attribute | Default | |
|---|---|---|
| `bootstrap_servers` | required | `list[str]` |
| `topic` | required | |
| `group_id` | required | where the offsets are stored |
| `batch_size` | `1_000` | records per poll, at least 1 |
| `poll_timeout_ms` | `10_000` | how long one poll waits for records |
| `min_batch_fill_ratio` | `0.1` | a poll under `batch_size * ratio` records ends `get_batches()`, 0 to 1 |
| `skip_undeserializable` | `False` | see below |
| `config` | `ConsumerConfig()` | passed to `AIOKafkaConsumer` as keyword arguments |

`get_batches(timestamp_threshold_minutes=5)` yields only messages older than five minutes, for consumers that
deliberately lag behind the producer. The boundary offset is looked up once per `get_batches()` call with
`offsets_for_times`; when no message in the partition is younger than the threshold the filter is off and
everything is read. Filtered records are not committed past, they are read next time.

A record that cannot be deserialized (a schema the consumer does not know, a payload written by something else)
raises `MessageDeserializationError` with `topic`, `partition` and `offset`, and the partition stays at that offset
until the group's offset is moved past it. A consumer whose stream can afford to lose such a record sets
`skip_undeserializable = True`: the record is logged with its offset and dropped, the rest of the batch goes on, and
the next `commit()` moves past it for good.

`ConsumerConfig` is a frozen pydantic model like `ProducerConfig`, with `extra='allow'` for any other aiokafka
argument. `enable_auto_commit` accepts only `False`: auto commit acknowledges records as soon as they are fetched,
which defeats `commit()` after a processed batch, commits past the records held back by
`timestamp_threshold_minutes` and moves past an undeserializable record silently. `session_timeout_ms` and
`heartbeat_interval_ms` are left out because the consumer does not join the group; they can still be passed.

| Field | Default | |
|---|---|---|
| `auto_offset_reset` | `'earliest'` | where a group without a committed offset starts |
| `enable_auto_commit` | `False` | only `False` |
| `request_timeout_ms` | `25_000` | |
| `retry_backoff_ms` | `1_000` | |
| `fetch_min_bytes` | `120_000` | the broker holds a fetch until this much or `fetch_max_wait_ms` |
| `fetch_max_bytes` | `125_000_000` | one fetch response |
| `fetch_max_wait_ms` | `1_000` | |
| `max_partition_fetch_bytes` | `25_000_000` | must exceed the largest record, otherwise the consumer is stuck on it |

## Consumer maker

`KafkaConsumerMaker` wraps a consumer in a distributed lock, so one partition is processed by one
worker at a time across the whole deployment. The lock is anything satisfying `Lockable`: `acquire()`, `release()`,
`extend()` and a `timeout` attribute, the seconds one `extend` is worth; `RedisLock` from
[ddredis](https://github.com/davyddd/ddredis) is one. The lock class is called with `key=` and `lock_kwargs`, so the
kwargs must not contain `key`; the key is `'{group_id}_{topic}_partition_{partition}'` (`build_lock_key`).

Entering acquires the lock and starts the consumer; every yielded batch is followed by a checkpoint (lock heartbeat +
offset commit); leaving commits, stops and releases. A lock that is already held raises from `acquire` with the
lock's own error (`AlreadyAcquiredError` for `RedisLock`), a consumer that fails to construct or start raises
`ConsumerStartError` after releasing the lock.

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

Every batch is committed as a whole after the caller is done with it. An exit caused by an exception does not
commit: the batch the caller failed on is read again next time, so processing must tolerate repeated records
(`idempotent_key`) and `batch_size` bounds how much is reprocessed. A failing `commit()` on exit still stops the
consumer and releases the lock. `maker.checkpoint()` commits the current batch, so call it yourself only once the
batch is processed.

With `stream=True` the consumer does not return when the topic runs dry: it keeps the lock alive and polls
again after `stream_round_pause` (default one second), so a cron that starts it acts as a supervisor and messages are
picked up as they land. The pace must fit the lock: `stream_round_pause` plus the consumer's poll timeout may not
exceed `MAX_BEAT_SHARE_OF_LOCK_TIMEOUT` (half) of the lock timeout, otherwise the maker raises `ValueError` at
construction. And the lock timeout is also the recovery time after a killed worker, since nothing releases its lock:
a streaming consumer wants a timeout measured in tens of seconds, not minutes.

```python
async with KafkaConsumerMaker(
    consumer_class=ProfileEventConsumer,
    partition=0,
    lock_class=RedisLock,
    lock_kwargs={'client_factory': redis_client_factory, 'timeout': 30},
    stream=True,
    stream_round_pause=timedelta(seconds=2),  # 2s + poll_timeout_ms 10s = 12s, within half of 30s
) as maker:
    async for batch in maker.get_batches():
        await process(batch)
```
