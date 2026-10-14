from datetime import timedelta
from typing import Any, ClassVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from aiokafka import TopicPartition

from ddkafka.config import ConsumerConfig
from ddkafka.consumer import BaseKafkaConsumer
from ddkafka.consumer_maker import ConsumerStartError, KafkaConsumerMaker
from tests.fakes import BOOTSTRAP_SERVERS, Event, FakeAIOKafkaConsumer, FakeLock, Record, packed


class EventConsumer(BaseKafkaConsumer[Event]):
    bootstrap_servers = BOOTSTRAP_SERVERS
    topic = 'events'
    group_id = 'analytics'
    batch_size = 10
    poll_timeout_ms = 1_000
    min_batch_fill_ratio = 0.5
    config: ClassVar[ConsumerConfig] = ConsumerConfig()


PARTITION = TopicPartition('events', 0)


class TestKafkaConsumerMaker(IsolatedAsyncioTestCase):
    def setUp(self):
        self.created: list[FakeAIOKafkaConsumer] = []

        def factory(**kwargs):
            self.created.append(FakeAIOKafkaConsumer(**kwargs))
            return self.created[-1]

        patcher = patch('ddkafka.consumer.AIOKafkaConsumer', side_effect=factory)
        patcher.start()
        self.addCleanup(patcher.stop)

    def make(self, **kwargs: Any) -> KafkaConsumerMaker[Event]:
        lock_kwargs = {'timeout': kwargs.pop('lock_timeout', 60.0), **kwargs.pop('lock_kwargs', {})}
        return KafkaConsumerMaker(
            consumer_class=EventConsumer, partition=0, lock_class=FakeLock, lock_kwargs=lock_kwargs, **kwargs
        )

    def test_lock_key_names_group_topic_and_partition(self):
        # Act & Assert
        self.assertEqual(KafkaConsumerMaker.build_lock_key(EventConsumer, 7), 'analytics_events_partition_7')
        self.assertEqual(self.make().lock.key, 'analytics_events_partition_0')

    async def test_enter_locks_and_starts_exit_commits_stops_and_releases(self):
        # Arrange
        maker = self.make()

        # Act
        async with maker as entered:
            kafka = self.created[-1]
            kafka.polls = [[Record(0, packed(1)), Record(1, packed(2))]]
            batches = [batch async for batch in entered.get_batches()]

        # Assert
        self.assertIs(entered, maker)
        self.assertEqual(len(batches), 1)
        self.assertTrue(kafka.started)
        self.assertTrue(kafka.stopped)
        # the checkpoint after the batch committed it; the commit on exit had nothing new
        self.assertEqual(kafka.committed, [{PARTITION: 2}])
        self.assertEqual(maker.lock.calls, ['acquire', 'extend', 'release'])

    async def test_each_checkpoint_commits_the_batch_just_processed(self):
        # Arrange
        maker = self.make()

        async with maker:
            kafka = self.created[-1]
            kafka.polls = [[Record(i, packed(i)) for i in range(10)], [Record(10, packed(10)), Record(11, packed(11))]]
            commits_seen: list[list[dict]] = []

            # Act: look at the commits right after each batch comes back
            async for _ in maker.get_batches():
                commits_seen.append(list(kafka.committed))

        # Assert: before batch 1 nothing; before batch 2 the checkpoint has committed batch 1
        self.assertEqual(commits_seen, [[], [{PARTITION: 10}]])
        self.assertEqual(kafka.committed, [{PARTITION: 10}, {PARTITION: 12}])

    async def test_failed_batch_is_not_committed(self):
        # Arrange
        maker = self.make()

        # Act
        with self.assertRaises(ValueError):
            async with maker:
                kafka = self.created[-1]
                kafka.polls = [[Record(i, packed(i)) for i in range(10)], [Record(10, packed(10))]]
                async for batch in maker.get_batches():
                    if batch[0].event_id == 10:  # noqa: PLR2004
                        raise ValueError('cannot process')

        # Assert: batch 1 was checkpointed, batch 2 stays uncommitted; the lock is released anyway
        self.assertEqual(kafka.committed, [{PARTITION: 10}])
        self.assertTrue(kafka.stopped)
        self.assertEqual(maker.lock.calls[-1], 'release')

    async def test_failed_start_releases_the_lock_and_raises_consumer_start_error(self):
        # Arrange
        maker = self.make()

        async def failing_start():
            raise ConnectionError('no broker')

        def factory(**kwargs):
            consumer = FakeAIOKafkaConsumer(**kwargs)
            consumer.start = failing_start  # type: ignore[method-assign]
            return consumer

        # Act & Assert
        with patch('ddkafka.consumer.AIOKafkaConsumer', side_effect=factory), self.assertRaises(ConsumerStartError) as ctx:
            async with maker:
                pass

        self.assertIsInstance(ctx.exception.__cause__, ConnectionError)
        self.assertEqual(maker.lock.calls, ['acquire', 'release'])

    async def test_failed_constructor_releases_the_lock(self):
        # Arrange: aiokafka refuses to build a consumer, e.g. outside a running loop
        maker = self.make()

        # Act & Assert
        with (
            patch('ddkafka.consumer.AIOKafkaConsumer', side_effect=RuntimeError('no event loop')),
            self.assertRaises(ConsumerStartError) as ctx,
        ):
            async with maker:
                pass

        self.assertIsInstance(ctx.exception.__cause__, RuntimeError)
        self.assertEqual(maker.lock.calls, ['acquire', 'release'])

    async def test_taken_lock_propagates_and_nothing_is_started(self):
        # Arrange
        maker = self.make(lock_kwargs={'fail_acquire': True})

        # Act & Assert
        with self.assertRaises(RuntimeError):
            async with maker:
                pass
        self.assertEqual(self.created, [])

    async def test_exit_releases_the_lock_even_when_commit_fails(self):
        # Arrange
        maker = self.make()

        async def failing_commit(_offsets):
            raise ConnectionError('lost')

        # Act & Assert
        with self.assertRaises(ConnectionError):
            async with maker:
                kafka = self.created[-1]
                kafka.commit = failing_commit  # type: ignore[method-assign]
                kafka.polls = [[Record(0, packed(1))]]
                async for _ in maker.get_batches():
                    pass

        self.assertTrue(kafka.stopped)
        self.assertEqual(maker.lock.calls[-1], 'release')

    def test_streaming_pace_must_fit_the_lock(self):
        # Act & Assert: pause 1s + poll 1s = 2s > 0.5 * 3s
        with self.assertRaisesRegex(ValueError, 'does not fit'):
            self.make(stream=True, lock_timeout=3.0)

        # 2s <= 0.5 * 4s is fine; the non-streaming maker never checks
        self.make(stream=True, lock_timeout=4.0)
        self.make(stream=False, lock_timeout=1.0)

    async def test_streaming_keeps_polling_after_the_topic_runs_dry(self):
        # Arrange
        maker = self.make(stream=True, lock_timeout=60.0, stream_round_pause=timedelta(0))

        async with maker:
            kafka = self.created[-1]
            kafka.polls = [[Record(0, packed(1))]]  # a thin batch: round 1 ends after it
            expected = [1, 2]
            seen: list[int] = []

            # Act: round 1 yields the batch, round 2 finds the partition empty, round 3 finds a new record
            async for batch in maker.get_batches():
                seen.extend(event.event_id for event in batch)
                if len(seen) == 1:
                    kafka.polls = [[], [Record(1, packed(2))]]
                if len(seen) == len(expected):
                    break

        # Assert
        self.assertEqual(seen, expected)
        # the empty round still produced a heartbeat
        self.assertGreaterEqual(maker.lock.calls.count('extend'), 3)
        self.assertEqual(maker.lock.calls[-1], 'release')

    async def test_non_streaming_returns_when_the_topic_runs_dry(self):
        # Arrange
        maker = self.make()

        async with maker:
            kafka = self.created[-1]
            kafka.polls = [[Record(i, packed(i)) for i in range(10)]]

            # Act
            batches = [batch async for batch in maker.get_batches()]

        # Assert
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(kafka.getmany_calls), 2)
