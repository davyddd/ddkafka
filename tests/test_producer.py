from typing import ClassVar
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

import msgpack

from ddkafka.config import ProducerConfig
from ddkafka.producer import BaseKafkaProducerRepository, serialize_key, serialize_value
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


class TestBaseKafkaProducerRepository(IsolatedAsyncioTestCase):
    def test_subclass_requires_bootstrap_servers(self):
        # Act & Assert
        with self.assertRaisesRegex(AttributeError, 'bootstrap_servers.*must be set'):

            class NoServers(BaseKafkaProducerRepository):
                pass

        with self.assertRaisesRegex(TypeError, 'bootstrap_servers.*must be list'):

            class StringServers(BaseKafkaProducerRepository):
                bootstrap_servers = 'kafka:9092'  # type: ignore[assignment]

    def test_subclass_requires_a_producer_config(self):
        # Act & Assert
        with self.assertRaisesRegex(TypeError, 'config.*must be ProducerConfig'):

            class DictConfig(BaseKafkaProducerRepository):
                bootstrap_servers = BOOTSTRAP_SERVERS
                config = {'acks': 1}  # type: ignore[assignment]

    async def test_producer_is_created_once_per_class_and_sends_serialized_events(self):
        # Arrange
        class EventProducerRepository(BaseKafkaProducerRepository):
            bootstrap_servers = BOOTSTRAP_SERVERS
            config: ClassVar[ProducerConfig] = ProducerConfig(linger_ms=5, enable_idempotence=True)

        created: list[FakeAIOKafkaProducer] = []

        def factory(**kwargs):
            created.append(FakeAIOKafkaProducer(**kwargs))
            return created[-1]

        with patch('ddkafka.producer.AIOKafkaProducer', side_effect=factory):
            # Act
            await EventProducerRepository().create('events', Event(event_id=1, name='a'))
            await EventProducerRepository().bulk_create('events', [Event(event_id=2, name='b'), Event(event_id=3, name='c')])

        # Assert
        self.assertEqual(len(created), 1)
        producer = created[0]
        self.assertTrue(producer.started)
        self.assertEqual(producer.kwargs['bootstrap_servers'], BOOTSTRAP_SERVERS)
        self.assertEqual(producer.kwargs['linger_ms'], 5)
        self.assertEqual(producer.kwargs['acks'], 'all')
        self.assertTrue(producer.kwargs['enable_idempotence'])
        self.assertEqual([key for _, key, _ in producer.sent], [b'1', b'2', b'3'])
        self.assertEqual(msgpack.loads(producer.sent[2][2]), {'event_id': 3, 'name': 'c'})
