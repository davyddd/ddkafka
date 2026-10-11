from unittest import TestCase

from pydantic import ValidationError

from ddkafka.config import ConsumerConfig, ProducerConfig


class TestProducerConfig(TestCase):
    def test_is_frozen(self):
        # Arrange
        config = ProducerConfig()

        # Act & Assert
        with self.assertRaises(ValidationError):
            config.acks = 1  # type: ignore[misc]

    def test_waits_for_all_in_sync_replicas_by_default(self):
        # Act
        kwargs = ProducerConfig().model_dump()

        # Assert
        self.assertEqual(kwargs['acks'], 'all')
        self.assertFalse(kwargs['enable_idempotence'])

    def test_acks_accepts_only_the_kafka_values(self):
        # Act & Assert
        for acks in (0, 1, 'all'):
            self.assertEqual(ProducerConfig(acks=acks).acks, acks)
        with self.assertRaises(ValidationError):
            ProducerConfig(acks=-1)  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            ProducerConfig(acks='1')  # type: ignore[arg-type]

    def test_idempotence_requires_acks_all(self):
        # Act & Assert
        with self.assertRaisesRegex(ValidationError, "acks='all'"):
            ProducerConfig(acks=1, enable_idempotence=True)

        ProducerConfig(acks='all', enable_idempotence=True)

    def test_other_aiokafka_arguments_are_forwarded(self):
        # Act
        kwargs = ProducerConfig(compression_type='gzip').model_dump()  # type: ignore[call-arg]

        # Assert
        self.assertEqual(kwargs['compression_type'], 'gzip')
        self.assertEqual(kwargs['acks'], 'all')

    def test_dumps_to_plain_kwargs(self):
        # Act
        kwargs = ProducerConfig(linger_ms=100).model_dump()

        # Assert
        self.assertEqual(kwargs['linger_ms'], 100)
        self.assertEqual(kwargs['max_batch_size'], 32_000)

    def test_every_setting_is_described(self):
        # Act & Assert
        for name, field in ProducerConfig.model_fields.items():
            self.assertTrue(field.description, name)


class TestConsumerConfig(TestCase):
    def test_is_frozen(self):
        # Arrange
        config = ConsumerConfig()

        # Act & Assert
        with self.assertRaises(ValidationError):
            config.enable_auto_commit = True  # type: ignore[misc]

    def test_commits_manually_from_the_beginning_by_default(self):
        # Act
        kwargs = ConsumerConfig().model_dump()

        # Assert
        self.assertFalse(kwargs['enable_auto_commit'])
        self.assertEqual(kwargs['auto_offset_reset'], 'earliest')
        self.assertLess(kwargs['heartbeat_interval_ms'] * 3, kwargs['session_timeout_ms'])
        self.assertGreater(kwargs['request_timeout_ms'], kwargs['session_timeout_ms'])

    def test_other_aiokafka_arguments_are_forwarded(self):
        # Act
        kwargs = ConsumerConfig(max_poll_records=500).model_dump()  # type: ignore[call-arg]

        # Assert
        self.assertEqual(kwargs['max_poll_records'], 500)

    def test_auto_commit_cannot_be_enabled(self):
        # Act & Assert
        with self.assertRaises(ValidationError):
            ConsumerConfig(enable_auto_commit=True)  # type: ignore[arg-type]

    def test_offset_reset_accepts_only_the_kafka_values(self):
        # Act & Assert
        with self.assertRaises(ValidationError):
            ConsumerConfig(auto_offset_reset='beginning')  # type: ignore[arg-type]

    def test_every_setting_is_described(self):
        # Act & Assert
        for name, field in ConsumerConfig.model_fields.items():
            self.assertTrue(field.description, name)
