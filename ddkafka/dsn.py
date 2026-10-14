from pydantic import UrlConstraints
from pydantic.networks import _BaseMultiHostUrl

DEFAULT_PORT = 9092


class KafkaDsn(_BaseMultiHostUrl):
    """`kafka://host[:port][,host[:port]]`, a settings field for the bootstrap servers of a cluster.

    The hosts are the brokers the client learns the rest of the cluster from; a missing port is 9092.
    Built like pydantic's own `PostgresDsn`, so a settings class validates it as a URL rather than
    parsing a JSON list by hand.
    """

    _constraints = UrlConstraints(allowed_schemes=['kafka'], host_required=True, default_port=DEFAULT_PORT)

    @property
    def bootstrap_servers(self) -> list[str]:
        """`['host:port', ...]`, the form `AIOKafkaProducer` / `AIOKafkaConsumer` take."""
        # `default_port` fills the port of a single host only; in a host list the others come back as None
        return [f'{host["host"]}:{host["port"] or DEFAULT_PORT}' for host in self.hosts()]


__all__ = ('KafkaDsn',)
