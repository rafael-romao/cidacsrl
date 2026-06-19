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
    request_timeout: NotRequired[int]
    msearch_batch_size: NotRequired[int]
    es_user: NotRequired[str]
    es_password: NotRequired[str]
    api_key: NotRequired[Union[str, Tuple[str, str]]]