import logging
from typing import NotRequired, Tuple, TypedDict, Union

logger = logging.getLogger("Entity: ElasticsearchConfig")


class ElasticsearchConfig(TypedDict):
    es_connection_url: str
    host: NotRequired[str]
    port: NotRequired[int]
    wan_only: NotRequired[bool]
    search_strategy: NotRequired[str]
    verify_certs: NotRequired[bool]
    ca_certs: NotRequired[str]
    client_cert: NotRequired[str]
    client_key: NotRequired[str]
    request_timeout: NotRequired[int]
    msearch_batch_size: NotRequired[int]
    es_user: NotRequired[str]
    es_password: NotRequired[str]
    api_key: NotRequired[Union[str, Tuple[str, str]]]