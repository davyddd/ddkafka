from unittest import TestCase

from pydantic import BaseModel, ValidationError

from ddkafka.dsn import KafkaDsn


class Settings(BaseModel):
    kafka_url: KafkaDsn


class TestKafkaDsn(TestCase):
    def test_single_host_gets_the_default_port(self):
        # Act
        settings = Settings(kafka_url='kafka://kafka')  # type: ignore[arg-type]

        # Assert
        self.assertEqual(settings.kafka_url.bootstrap_servers, ['kafka:9092'])

    def test_several_hosts_with_and_without_ports(self):
        # Act
        settings = Settings(kafka_url='kafka://kafka-0:9092,kafka-1,kafka-2:9093')  # type: ignore[arg-type]

        # Assert
        self.assertEqual(settings.kafka_url.bootstrap_servers, ['kafka-0:9092', 'kafka-1:9092', 'kafka-2:9093'])

    def test_other_schemes_and_missing_host_are_rejected(self):
        # Act & Assert
        with self.assertRaises(ValidationError):
            Settings(kafka_url='redis://kafka:9092')  # type: ignore[arg-type]
        with self.assertRaises(ValidationError):
            Settings(kafka_url='kafka://')  # type: ignore[arg-type]

    def test_serializes_back_to_the_url(self):
        # Act
        settings = Settings(kafka_url='kafka://kafka-0:9092,kafka-1:9093')  # type: ignore[arg-type]

        # Assert
        self.assertEqual(str(settings.kafka_url), 'kafka://kafka-0:9092,kafka-1:9093')
        self.assertEqual(settings.model_dump(mode='json')['kafka_url'], 'kafka://kafka-0:9092,kafka-1:9093')
