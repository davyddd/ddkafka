import asyncio
import os
from functools import partial
from typing import cast
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import msgpack
from aiokafka import AIOKafkaProducer
from aiokafka.errors import NotEnoughReplicasError
from ddutils.connections import ConnectionManagerFactory
from ddutils.scoped_registry import ScopedRegistry

from ddkafka.config import ProducerConfig
from ddkafka.producer import BaseKafkaProducer, create_producer, serialize_key, serialize_value
from tests.fakes import BOOTSTRAP_SERVERS, Event, FakeAIOKafkaProducer


class TestSerializers(IsolatedAsyncioTestCase):
    def test_value_is_msgpack_of_the_json_dump(self):
        # Arrange
        event = Event(event_id=1, name='a')

        # Act & Assert
        self.assertEqual(msgpack.loads(serialize_value(event)), {'event_id': 1, 'name': 'a'})

    def test_key_is_utf8_and_none_stays_none(self):
        # Act & Assert
        self.assertEqual(serialize_key(42), b'42')
        self.assertEqual(serialize_key('k'), b'k')
        self.assertIsNone(serialize_key(None))


class TestCreateProducer(IsolatedAsyncioTestCase):
    async def test_builds_a_started_producer_with_the_serializers_and_config(self):
        # Arrange
        created: list[FakeAIOKafkaProducer] = []

        def factory(**kwargs):
            created.append(FakeAIOKafkaProducer(**kwargs))
            return created[-1]

        # Act
        with patch('ddkafka.producer.AIOKafkaProducer', side_effect=factory):
            producer = await create_producer(BOOTSTRAP_SERVERS, ProducerConfig(linger_ms=5, enable_idempotence=True))

        # Assert
        fake = created[0]
        self.assertIs(producer, fake)
        self.assertTrue(fake.started)
        self.assertEqual(fake.kwargs['bootstrap_servers'], BOOTSTRAP_SERVERS)
        self.assertEqual(fake.kwargs['linger_ms'], 5)
        self.assertEqual(fake.kwargs['acks'], 'all')
        self.assertTrue(fake.kwargs['enable_idempotence'])
        self.assertIs(fake.kwargs['value_serializer'], serialize_value)
        self.assertIs(fake.kwargs['key_serializer'], serialize_key)


class TestBaseKafkaProducer(IsolatedAsyncioTestCase):
    def setUp(self):
        self.created: list[FakeAIOKafkaProducer] = []

        def factory(**kwargs):
            self.created.append(FakeAIOKafkaProducer(**kwargs))
            return self.created[-1]

        patcher = patch('ddkafka.producer.AIOKafkaProducer', side_effect=factory)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.registries = {
            alias: ScopedRegistry[AIOKafkaProducer](
                create_func=partial(create_producer, [f'{alias}:9092']), scope_func=os.getpid, destructor_method_name='stop'
            )
            for alias in ('primary', 'replica')
        }
        self.factory = ConnectionManagerFactory(self.registries, default='primary')

        class EventProducer(BaseKafkaProducer):
            producer_factory = self.factory

        self.producer_class = EventProducer

    async def asyncTearDown(self):
        await self.factory.close()

    def test_subclass_requires_a_factory(self):
        # Act & Assert
        with self.assertRaisesRegex(AttributeError, 'producer_factory.*must be set'):

            class NoFactory(BaseKafkaProducer):
                pass

        with self.assertRaisesRegex(TypeError, 'producer_factory.*must be ConnectionManagerFactory'):

            class RegistryInstead(BaseKafkaProducer):
                producer_factory = self.registries['primary']  # type: ignore[assignment]

    async def test_alias_is_passed_to_the_factory(self):
        # Arrange
        class ReplicaProducer(self.producer_class):  # type: ignore[misc]
            alias = 'replica'

        # Act
        await self.producer_class().create('events', Event(event_id=1, name='a'))
        await ReplicaProducer().create('events', Event(event_id=2, name='b'))

        # Assert: one producer per cluster, each sent to through its own registry
        self.assertEqual([p.kwargs['bootstrap_servers'] for p in self.created], [['primary:9092'], ['replica:9092']])

    async def test_sends_serialized_events_through_the_factory_producer(self):
        # Act
        await self.producer_class().create('events', Event(event_id=1, name='a'))
        await self.producer_class().bulk_create('events', [Event(event_id=2, name='b'), Event(event_id=3, name='c')])

        # Assert: one producer for every instance of the class
        self.assertEqual(len(self.created), 1)
        producer = self.created[0]
        self.assertEqual([key for _, key, _ in producer.sent], [b'1', b'2', b'3'])
        self.assertEqual(msgpack.loads(producer.sent[2][2]), {'event_id': 3, 'name': 'c'})

    async def test_concurrent_first_sends_start_one_producer(self):
        # Arrange
        repo = self.producer_class()
        events = [Event(event_id=i, name='x') for i in range(5)]

        # Act
        await asyncio.gather(*(repo.create('events', event) for event in events))

        # Assert
        self.assertEqual(len(self.created), 1)
        self.assertEqual(len(self.created[0].sent), 5)

    async def test_closing_the_factory_stops_the_producer(self):
        # Arrange
        await self.producer_class().create('events', Event(event_id=1, name='a'))

        # Act
        await self.factory.close()

        # Assert
        self.assertTrue(self.created[0].stopped)

    async def test_rejected_delivery_is_logged_and_accepted_one_is_silent(self):
        # Arrange
        await self.producer_class().bulk_create('events', [Event(event_id=1, name='a'), Event(event_id=2, name='b')])
        producer = self.created[0]

        # Act
        with self.assertLogs('ddkafka.producer', level='ERROR') as logs:
            producer.deliveries[0].set_result('record metadata')
            producer.deliveries[1].set_exception(NotEnoughReplicasError('isr too small'))
            await asyncio.sleep(0)  # let the done callbacks run

        # Assert
        self.assertEqual(len(logs.records), 1)
        entry = cast('dict', logs.records[0].msg)
        self.assertEqual(entry['topic'], 'events')
        self.assertEqual(entry['key'], '2')
        self.assertIn('NotEnoughReplicasError', entry['error'])
