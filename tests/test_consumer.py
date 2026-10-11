from types import new_class
from typing import ClassVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndTimestamp
from pydantic import BaseModel

from ddkafka.config import ConsumerConfig
from ddkafka.consumer import BaseKafkaConsumerRepository
from tests.fakes import BOOTSTRAP_SERVERS, Event, FakeAIOKafkaConsumer, Record, packed


class EventConsumerRepository(BaseKafkaConsumerRepository[Event]):
    bootstrap_servers = BOOTSTRAP_SERVERS
    topic = 'events'
    group_id = 'analytics'
    batch_size = 10
    poll_timeout_ms = 500
    min_batch_fill_ratio = 0.5
    config: ClassVar[ConsumerConfig] = ConsumerConfig(fetch_min_bytes=1)


PARTITION = TopicPartition('events', 0)


class TestConsumerSubclassing(IsolatedAsyncioTestCase):
    def test_domain_type_is_taken_from_the_generic_base(self):
        # Act & Assert
        self.assertIs(EventConsumerRepository._domain_class, Event)

    def test_domain_type_is_required(self):
        # Act & Assert
        with self.assertRaises(TypeError):

            class Untyped(BaseKafkaConsumerRepository):  # type: ignore[type-arg]
                bootstrap_servers = BOOTSTRAP_SERVERS
                topic = 'events'
                group_id = 'analytics'

    def test_required_attributes(self):
        # Arrange
        cases = {
            'bootstrap_servers': (None, AttributeError, 'must be set'),
            'topic': ('', AttributeError, 'must not be empty'),
            'group_id': (1, TypeError, 'must be str, got int'),
            'batch_size': (0, ValueError, 'must be positive'),
            'poll_timeout_ms': (1.5, TypeError, 'must be int, got float'),
            'min_batch_fill_ratio': (2.0, ValueError, 'between 0 and 1'),
            'config': ({}, AttributeError, 'must not be empty'),
        }
        base_attributes = {'bootstrap_servers': BOOTSTRAP_SERVERS, 'topic': 'events', 'group_id': 'analytics'}

        for name, (bad_value, error, message) in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(error, f'`{name}`.*{message}'):
                attributes = {**base_attributes, name: bad_value}
                # Act
                new_class(
                    'Broken', (BaseKafkaConsumerRepository[Event],), exec_body=lambda ns, attrs=attributes: ns.update(attrs)
                )

    def test_zero_poll_timeout_is_allowed(self):
        # Act
        class Impatient(BaseKafkaConsumerRepository[Event]):
            bootstrap_servers = BOOTSTRAP_SERVERS
            topic = 'events'
            group_id = 'analytics'
            poll_timeout_ms = 0
            min_batch_fill_ratio = 0.0

        # Assert
        self.assertEqual(Impatient.poll_timeout_ms, 0)


class TestConsumerLifecycle(IsolatedAsyncioTestCase):
    def setUp(self):
        self.created: list[FakeAIOKafkaConsumer] = []

        def factory(**kwargs):
            self.created.append(FakeAIOKafkaConsumer(**kwargs))
            return self.created[-1]

        patcher = patch('ddkafka.consumer.AIOKafkaConsumer', side_effect=factory)
        patcher.start()
        self.addCleanup(patcher.stop)

    @property
    def kafka(self) -> FakeAIOKafkaConsumer:
        return self.created[-1]

    def test_consumer_is_assigned_to_the_partition_with_the_config(self):
        # Act
        EventConsumerRepository(partition=3)

        # Assert
        self.assertEqual(self.kafka.assigned, [TopicPartition('events', 3)])
        self.assertEqual(self.kafka.kwargs['group_id'], 'analytics')
        self.assertEqual(self.kafka.kwargs['bootstrap_servers'], BOOTSTRAP_SERVERS)
        self.assertEqual(self.kafka.kwargs['fetch_min_bytes'], 1)
        self.assertFalse(self.kafka.kwargs['enable_auto_commit'])

    async def test_start_and_stop(self):
        # Arrange
        repo = EventConsumerRepository(partition=0)

        # Act
        await repo.start()
        await repo.stop()

        # Assert
        self.assertTrue(self.kafka.started)
        self.assertTrue(self.kafka.stopped)

    async def test_messages_are_deserialized_into_the_domain_type(self):
        # Arrange
        repo = EventConsumerRepository(partition=0)
        self.kafka.polls = [[Record(0, packed(1, 'a')), Record(1, packed(2, 'b'))]]

        # Act
        batches = [batch async for batch in repo.get_batches()]

        # Assert
        self.assertEqual(batches, [(Event(event_id=1, name='a'), Event(event_id=2, name='b'))])
        self.assertEqual(self.kafka.getmany_calls, [{'timeout_ms': 500, 'max_records': 10}])

    async def test_full_batches_keep_polling_and_a_thin_one_ends_the_run(self):
        # Arrange: 10 (full), 10 (full), 3 (< 10 * 0.5) -> stop without a fourth poll
        repo = EventConsumerRepository(partition=0)
        self.kafka.polls = [
            [Record(i, packed(i)) for i in range(10)],
            [Record(i, packed(i)) for i in range(10, 20)],
            [Record(i, packed(i)) for i in range(20, 23)],
            [Record(i, packed(i)) for i in range(23, 33)],  # never reached
        ]

        # Act
        sizes = [len(batch) async for batch in repo.get_batches()]

        # Assert
        self.assertEqual(sizes, [10, 10, 3])
        self.assertEqual(len(self.kafka.polls), 1)

    async def test_empty_poll_ends_the_run(self):
        # Arrange
        repo = EventConsumerRepository(partition=0)
        self.kafka.polls = [[Record(i, packed(i)) for i in range(10)]]

        # Act
        sizes = [len(batch) async for batch in repo.get_batches()]

        # Assert: the full batch asked for another poll, which came back empty
        self.assertEqual(sizes, [10])
        self.assertEqual(len(self.kafka.getmany_calls), 2)

    async def test_commit_acknowledges_the_batch_being_processed(self):
        # Arrange
        repo = EventConsumerRepository(partition=0)
        self.kafka.polls = [[Record(i, packed(i)) for i in range(10)], [Record(10, packed(10)), Record(11, packed(11))]]

        # Act: commit while still inside the loop, before asking for the next batch
        commits_during_loop = []
        async for _ in repo.get_batches():
            await repo.commit()
            commits_during_loop.append(list(self.kafka.committed))
        await repo.commit()  # nothing new -> no extra commit call

        # Assert
        self.assertEqual(commits_during_loop, [[{PARTITION: 10}], [{PARTITION: 10}, {PARTITION: 12}]])
        self.assertEqual(self.kafka.committed, [{PARTITION: 10}, {PARTITION: 12}])

    async def test_timestamp_threshold_limits_the_offsets(self):
        # Arrange: only records older than the threshold (offset < 12) are yielded
        repo = EventConsumerRepository(partition=0)
        self.kafka.offsets_for_times_result = {PARTITION: OffsetAndTimestamp(12, 0)}
        self.kafka.polls = [
            [Record(i, packed(i)) for i in range(10)],
            [Record(i, packed(i)) for i in range(10, 20)],
        ]

        # Act
        batches = [[event.event_id for event in batch] async for batch in repo.get_batches(timestamp_threshold_minutes=5)]
        await repo.commit()

        # Assert
        self.assertEqual(batches, [list(range(10)), [10, 11]])
        self.assertEqual(self.kafka.committed, [{PARTITION: 12}])

    async def test_threshold_with_no_older_messages_yields_nothing(self):
        # Arrange
        repo = EventConsumerRepository(partition=0)
        self.kafka.offsets_for_times_result = {PARTITION: OffsetAndTimestamp(0, 0)}
        self.kafka.polls = [[Record(0, packed(1)), Record(1, packed(2))]]

        # Act
        batches = [batch async for batch in repo.get_batches(timestamp_threshold_minutes=5)]
        await repo.commit()

        # Assert
        self.assertEqual(batches, [])
        self.assertEqual(self.kafka.committed, [])

    async def test_threshold_without_a_matching_offset_disables_the_filter(self):
        # Arrange: Kafka returns None when every message is older than the threshold
        repo = EventConsumerRepository(partition=0)
        self.kafka.offsets_for_times_result = {PARTITION: None}
        self.kafka.polls = [[Record(0, packed(1)), Record(1, packed(2))]]

        # Act
        sizes = [len(batch) async for batch in repo.get_batches(timestamp_threshold_minutes=5)]

        # Assert
        self.assertEqual(sizes, [2])


class TestOtherDeserializable(IsolatedAsyncioTestCase):
    async def test_any_class_with_model_validate_works(self):
        # Arrange
        class Plain:
            def __init__(self, data: dict):
                self.data = data

            @classmethod
            def model_validate(cls, data: dict) -> 'Plain':
                return cls(data)

        class PlainConsumer(BaseKafkaConsumerRepository[Plain]):
            bootstrap_servers = BOOTSTRAP_SERVERS
            topic = 'events'
            group_id = 'g'

        with patch('ddkafka.consumer.AIOKafkaConsumer', side_effect=lambda **kw: FakeAIOKafkaConsumer(**kw)):
            repo = PlainConsumer(partition=0)
            repo._consumer.polls = [[Record(0, packed(7, 'z'))]]  # type: ignore[attr-defined]

            # Act
            batches = [batch async for batch in repo.get_batches()]

        # Assert
        self.assertEqual(batches[0][0].data, {'event_id': 7, 'name': 'z'})
        self.assertIsInstance(batches[0][0], Plain)
        self.assertFalse(issubclass(Plain, BaseModel))
