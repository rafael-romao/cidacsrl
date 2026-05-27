import logging
from typing import TypedDict, NotRequired, Union, Tuple


logger = logging.getLogger(__name__)


class ElasticsearchConfig(TypedDict):
    es_connection_url: str
    verify_certs: NotRequired[bool]
    request_timeout: NotRequired[int]
    msearch_batch_size: NotRequired[int]
    es_user: NotRequired[str]
    es_password: NotRequired[str]
    api_key: NotRequired[Union[str, Tuple[str, str]]]