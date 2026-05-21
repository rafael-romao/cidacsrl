import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ElasticsearchServiceConfig:
    """
    Configuração de conexão com o serviço Elasticsearch.

    Atributos:
        es_connection_url (str): URL base do cluster ES (ex: "http://elasticsearch:9200").
        verify_certs (bool): Se deve verificar certificados TLS (default: True).
        request_timeout (int): Timeout em segundos para requisições ao ES (default: 60).
        msearch_batch_size (int): Número máximo de requisições por chamada msearch (default: 100).
    """
    es_connection_url: str
    verify_certs: bool = True
    request_timeout: int = 60
    msearch_batch_size: int = 100

    def __post_init__(self):
        if not self.es_connection_url:
            raise ValueError("'es_connection_url' não pode ser vazio.")
        if not self.es_connection_url.startswith(("http://", "https://")):
            raise ValueError(
                f"'es_connection_url' inválida: '{self.es_connection_url}'. "
                "Deve começar com 'http://' ou 'https://'."
            )
        if self.request_timeout <= 0:
            raise ValueError("'request_timeout' deve ser um valor positivo.")
        if self.msearch_batch_size <= 0:
            raise ValueError("'msearch_batch_size' deve ser um valor positivo.")
