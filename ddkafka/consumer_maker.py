import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any, Generic, Protocol, Self, TypeVar

from ddkafka.consumer import BaseKafkaConsumer, Deserializable


class Lockable(Protocol):
    # How long one `extend` is worth. A streaming consumer has to beat faster than this, so the
    # contract has to say it out loud rather than leave it to whoever wires the lock up.
    timeout: float

    async def acquire(self) -> None: ...

    async def release(self) -> None: ...

    async def extend(self) -> None: ...


class ConsumerStartError(Exception): ...


# How much of the lock's timeout a streaming consumer is allowed to spend between two beats.
# The rest is headroom for the one thing the check cannot see — how long the caller takes over
# a batch, which is bounded by nothing here.
MAX_BEAT_SHARE_OF_LOCK_TIMEOUT = 0.5

DomainT = TypeVar('DomainT', bound=Deserializable)


class KafkaConsumerMaker(Generic[DomainT]):
    """
    Context manager for a Kafka consumer guarded by a distributed lock.

    The lock (anything satisfying `Lockable`, e.g. `ddredis.lock.RedisLock`) is keyed by group,
    topic and partition, so only one consumer per partition runs across all workers. Entering
    acquires the lock and starts the consumer; every batch yielded by `get_batches` is followed
    by a checkpoint (lock heartbeat + offset commit); leaving commits, stops and releases. An exit
    caused by an exception does not commit, so the batch being processed is read again next time.

    `stream` decides what happens when the topic runs dry. Without it the consumer drains what
    is waiting and returns, which is what a periodic sync wants. With it the consumer stays on
    its partition indefinitely, and the cron that starts it becomes a supervisor rather than a
    schedule: every tick tries to start one, the lock turns all but the first away, and a tick
    only does anything once the running consumer has died. Recovery from a death that leaves
    the lock held (a killed worker) takes the lock's own `timeout`, so a streaming consumer
    wants that timeout measured in tens of seconds, not minutes.

    `stream_round_pause` is how long it waits between rounds — the topic's chance to refill,
    and the ceiling on how long a message can sit once it has arrived. A consumer whose topic
    trickles wants it longer, one whose latency is the point wants it short. It is bounded from
    above by the lock: pause plus poll timeout is the longest the lock can go unextended, and a
    pace that does not fit is refused at construction.

    Example:
        async with KafkaConsumerMaker(
            consumer_class=EventConsumer,
            partition=0,
            lock_class=RedisLock,
            lock_kwargs={'redis_client': redis_client, 'timeout': 300}
        ) as maker:
            async for batch in maker.get_batches():
                process(batch)
    """

    def __init__(
        self,
        consumer_class: type[BaseKafkaConsumer[DomainT]],
        partition: int,
        lock_class: type[Lockable],
        lock_kwargs: dict[str, Any],
        stream: bool = False,
        stream_round_pause: timedelta = timedelta(seconds=1),
    ):
        lock_key = self.build_lock_key(consumer_class, partition)
        self.lock = lock_class(key=lock_key, **lock_kwargs)  # type: ignore[call-arg]
        self._consumer_class = consumer_class
        self._partition = partition
        self._stream = stream
        self._stream_round_pause = stream_round_pause

        if stream:
            self._check_beat_fits_lock()

    def _check_beat_fits_lock(self) -> None:
        """Refuse a pace that would let the lock die under a consumer that is still running.

        A streaming consumer beats once per round, and a round is the pause plus a poll that may
        wait its whole timeout out. Let that exceed the lock and the lock expires between beats:
        the next tick then starts a second consumer on the same partition, both read it and both
        commit, and nothing in the running system says so.
        """
        poll_timeout = self._consumer_class.poll_timeout_ms / 1000
        beat_ceiling = self._stream_round_pause.total_seconds() + poll_timeout
        allowed = self.lock.timeout * MAX_BEAT_SHARE_OF_LOCK_TIMEOUT
        if beat_ceiling > allowed:
            raise ValueError(
                f'{self._consumer_class.__name__} streams with up to {beat_ceiling:g}s between lock '
                f'heartbeats (stream_round_pause {self._stream_round_pause.total_seconds():g}s + poll '
                f'timeout {poll_timeout:g}s), which does not fit the '
                f'{MAX_BEAT_SHARE_OF_LOCK_TIMEOUT:g} share of the lock timeout it is allowed '
                f'({allowed:g}s of {self.lock.timeout:g}s). Shorten the pause or raise the lock timeout.'
            )

    @staticmethod
    def build_lock_key(consumer_class: type[BaseKafkaConsumer[Any]], partition: int) -> str:
        return f'{consumer_class.group_id}_{consumer_class.topic}_partition_{partition}'

    async def get_batches(self) -> AsyncIterator[tuple[DomainT, ...]]:
        while True:
            async for batch in self.consumer.get_batches():
                yield batch
                await self.checkpoint()

            if not self._stream:
                return

            # The topic went quiet. Returning here would hand the partition back and leave the
            # next message to whatever starts a consumer again — a cron tick, and with it a
            # whole stage of the send path's latency. Instead the lock is kept alive and the
            # consumer asked once more: its own poll timeout is the wait, so an idle partition
            # costs one call every few seconds and a message is taken up as it lands.
            #
            # A checkpoint rather than a bare `extend`, so the heartbeat has one call site: a
            # round that found nothing yields no batch, so without this an idle partition would
            # have no heartbeat at all and the lock would expire under a running consumer.
            await self.checkpoint()

            # The pause lets records accumulate into fuller batches and is the floor under a
            # poll that returns instantly, which would otherwise spin.
            await asyncio.sleep(self._stream_round_pause.total_seconds())

    async def checkpoint(self) -> None:
        """
        Extends the lock and commits every batch yielded so far, including the one being processed.

        `get_batches` calls it after the caller is done with a batch. Calling it yourself from
        inside the loop commits the current batch before it is processed: if the processing then
        fails, those records are gone.
        """
        await self.lock.extend()
        await self.consumer.commit()

    async def __aenter__(self) -> Self:
        await self.lock.acquire()

        # Anything that fails after the lock is taken has to give it back, or the partition stays
        # locked until the lock's timeout: the constructor (aiokafka needs a running loop) as much
        # as `start` (no broker).
        try:
            self.consumer = self._consumer_class(partition=self._partition)
            await self.consumer.start()
        except Exception as e:  # noqa: BLE001
            await self._close()
            raise ConsumerStartError('Failed to start consumer') from e

        return self

    async def _close(self) -> None:
        try:
            if hasattr(self, 'consumer'):
                await self.consumer.stop()
        finally:
            await self.lock.release()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # The offsets of a yielded batch are already recorded, so an exit caused by an error in
        # the caller must not commit: the batch it failed on stays in the topic for the next run.
        try:
            if exc_type is None:
                await self.consumer.commit()
        finally:
            await self._close()


__all__ = ('ConsumerStartError', 'KafkaConsumerMaker', 'Lockable')
