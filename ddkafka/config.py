from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProducerConfig(BaseModel):
    """Keyword arguments passed to `aiokafka.AIOKafkaProducer` as they are (`model_dump()`).

    Any other `AIOKafkaProducer` keyword argument (`compression_type`, `max_request_size`, ...) can be
    passed as well and is forwarded untouched.

    The defaults assume a cluster set up for durability (`default.replication.factor=3`,
    `min.insync.replicas=2`): `acks='all'` is the client half of that guarantee, the rest is batching
    tuned for a steady event stream rather than single sends.
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    acks: Literal[0, 1, 'all'] = Field(
        default='all',
        description=(
            "How many brokers must confirm a record before `send` resolves. `'all'` waits for every in-sync "
            "replica, which is the only setting where the cluster's `min.insync.replicas` applies: with "
            'replication factor 3 and min ISR 2 a confirmed record survives the loss of one broker. `1` is '
            'confirmed by the leader alone and is lost if the leader dies before a follower copies it; `0` '
            'does not wait at all. Replication and min ISR are cluster settings chosen by devops; overriding '
            '`acks` silently changes the delivery guarantee they set up, so do it knowingly. Note that '
            "`'all'` needs the topic to actually have at least `min.insync.replicas` replicas: a topic "
            'created with one replica rejects every write with `NotEnoughReplicas`.'
        ),
    )
    enable_idempotence: bool = Field(
        default=False,
        description=(
            'Exactly one copy of each record even when a send is retried after a broker failure. Requires '
            "`acks='all'` and costs a sequence number per partition. Off by default: records are keyed with "
            '`idempotent_key`, so a duplicate is cheap to drop on the consumer side, and consumers are '
            'at-least-once anyway.'
        ),
    )
    linger_ms: int = Field(
        default=50,
        description=(
            'How long the producer waits for more records before sending a batch. 0 sends each record at '
            'once; a small delay lets the records of one request share a batch, which is what makes '
            '`bulk_create` cheap.'
        ),
    )
    max_batch_size: int = Field(
        default=32_000,
        description='Bytes per partition batch. A batch is sent when it reaches this size or `linger_ms` passes.',
    )
    retry_backoff_ms: int = Field(
        default=500,
        description='Pause before retrying a failed request (leader change, transient broker error).',
    )
    metadata_max_age_ms: int = Field(
        default=30_000,
        description=(
            'How often the producer refreshes cluster metadata even without errors, so a new partition or a '
            'moved leader is picked up within this time.'
        ),
    )
    request_timeout_ms: int = Field(
        default=5_000,
        description=(
            "How long a produce request may wait for the broker's response before it fails. With `acks='all'` "
            "this includes the followers catching up, so keep it above the broker's `replica.lag.time.max.ms`."
        ),
    )

    @model_validator(mode='after')
    def _idempotence_requires_acks_all(self) -> Self:
        if self.enable_idempotence and self.acks != 'all':
            raise ValueError("`enable_idempotence=True` requires `acks='all'`")
        return self


class ConsumerConfig(BaseModel):
    """Keyword arguments passed to `aiokafka.AIOKafkaConsumer` as they are (`model_dump()`).

    Any other `AIOKafkaConsumer` keyword argument (`max_poll_records`, `isolation_level`, ...) can be
    passed as well and is forwarded untouched.

    The defaults make a batch consumer: offsets are committed by the repository after a batch is
    processed (at-least-once) and fetches wait for a meaningful amount of data.

    The repository assigns its partition explicitly (`assign`, not `subscribe`), so the consumer never
    joins the group: `session_timeout_ms`, `heartbeat_interval_ms` and the other group membership
    settings have no effect here and are left out. They can still be passed if a consumer subscribes.
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    auto_offset_reset: Literal['earliest', 'latest', 'none'] = Field(
        default='earliest',
        description=(
            'Where a group starts when it has no committed offset for a partition (first run, or the '
            "committed offset was deleted by retention): `'earliest'` reads the whole retained history, "
            "`'latest'` only new records, `'none'` raises and leaves the decision to the caller."
        ),
    )
    # Only False is accepted. Auto commit acknowledges the consumer's position, the offset right after the
    # last record `getmany` returned, every `auto_commit_interval_ms`, regardless of what the application
    # has done with those records. That breaks everything the repository relies on: `commit()` after a
    # processed batch (at-least-once) becomes meaningless, records filtered out by
    # `timestamp_threshold_minutes` get committed past and are never read again, and a partition that
    # must stay on an undeserializable record is silently moved past it.
    enable_auto_commit: Literal[False] = Field(
        default=False,
        description=(
            'Must stay False: `BaseKafkaConsumerRepository.commit` acknowledges offsets after a batch is '
            'processed. Auto commit would acknowledge records as soon as they are fetched, and a crash in the '
            'middle of a batch would lose them.'
        ),
    )
    request_timeout_ms: int = Field(
        default=25_000,
        description='How long a request to the broker may wait for a response.',
    )
    retry_backoff_ms: int = Field(
        default=1_000,
        description='Pause before retrying a failed request (coordinator moved, transient broker error).',
    )
    fetch_min_bytes: int = Field(
        default=120_000,
        description=(
            'The broker holds a fetch until this many bytes are available or `fetch_max_wait_ms` passes. '
            'A high value turns a trickle of small records into fewer, fuller batches.'
        ),
    )
    fetch_max_bytes: int = Field(
        default=125_000_000,
        description='Upper bound on one fetch response across all partitions the consumer reads.',
    )
    fetch_max_wait_ms: int = Field(
        default=1_000,
        description='How long the broker may hold a fetch waiting for `fetch_min_bytes`.',
    )
    max_partition_fetch_bytes: int = Field(
        default=25_000_000,
        description=(
            'Upper bound on the data returned for one partition in one fetch. Must be at least the largest '
            'record in the topic, otherwise the consumer is stuck on it.'
        ),
    )


__all__ = ('ConsumerConfig', 'ProducerConfig')
