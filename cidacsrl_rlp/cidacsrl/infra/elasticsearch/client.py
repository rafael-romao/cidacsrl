# src/es/client.py

import logging
from typing import Any, Dict, Optional, Tuple, List, Union

from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError, AuthenticationException, AuthorizationException

logger = logging.getLogger(__name__)

# Global cache for Elasticsearch clients within the same Python process (e.g., a Spark executor).
# Keyed by a string generated from essential connection parameters.
_es_clients: Dict[str, Elasticsearch] = {}


def _generate_cache_key(config: Dict[str, Any]) -> str:
    """
    Gera uma chave de cache única baseada nos parâmetros essenciais de conexão do Elasticsearch.
    Inclui URL, usuário, timeout, verify_certs para garantir clientes distintos para configurações distintas.
    Outros parâmetros críticos de conexão (ca_certs, etc.) podem ser adicionados se necessário.

    Args:
        config (Dict[str, Any]): Dicionário de configuração do Elasticsearch.

    Returns:
        str: Uma chave de cache única.
    """
    # List of configuration keys that define a unique connection.
    # Order is important for key consistency.
    key_defining_params = [
        "es_connection_url",
        "es_user", # Password is not included in the key for security/simplicity
        "cloud_id",
        "api_key", # If using API Key authentication
        "request_timeout",
        "verify_certs",
        "ca_certs",
        "client_cert",
        "client_key"
    ]

    key_parts = []
    for param_name in sorted(key_defining_params): # Sort to ensure key consistency
        value = config.get(param_name)
        if value is not None:
            key_parts.append(f"{param_name}={value}")

    return "&".join(key_parts)


def get_es_client(config: Dict[str, Any], use_cache: bool = True) -> Optional[Elasticsearch]:
    """
    Inicializa e retorna um cliente Elasticsearch baseado na configuração fornecida.
    Utiliza um cache local ao processo para reutilizar clientes com a mesma configuração
    se `use_cache` for True.

    Args:
        config (Dict[str, Any]): Dicionário contendo a configuração do Elasticsearch. Chaves esperadas incluem:
            - "es_connection_url" (Union[str, List[str]]): URL(s) base para o(s) nó(s) Elasticsearch.
                                                           (Obrigatório se "cloud_id" não for fornecido).
            - "cloud_id" (Optional[str]): Cloud ID para conexões Elastic Cloud.
            - "api_key" (Optional[Union[str, Tuple[str, str]]]): Chave API para autenticação.
            - "es_user" (Optional[str]): Usuário para autenticação básica.
            - "es_password" (Optional[str]): Senha para autenticação básica.
            - "verify_certs" (Optional[bool]): Verificar certificados SSL (padrão: True).
            - "ca_certs" (Optional[str]): Caminho para o arquivo CA cert.
            - "client_cert" (Optional[str]): Caminho para o arquivo client cert.
            - "client_key" (Optional[str]): Caminho para o arquivo client key.
            - "request_timeout" (Optional[int]): Timeout em segundos para requisições (padrão: 60).
            - Outros parâmetros válidos do cliente Elasticsearch (ex: http_compress, max_retries).
        use_cache (bool): Se True (padrão), tenta retornar um cliente do cache para a mesma configuração
                          dentro do processo Python atual. Se False, sempre cria um novo cliente.

    Returns:
        Optional[Elasticsearch]: Uma instância do cliente Elasticsearch se bem-sucedido, caso contrário `None`.

    Raises:
        ValueError: Se parâmetros de configuração essenciais estiverem ausentes ou inválidos.
    """
    if not config.get("es_connection_url") and not config.get("cloud_id"):
        logger.error("Elasticsearch configuration must contain 'es_connection_url' or 'cloud_id'.")
        raise ValueError("Elasticsearch configuration must contain 'es_connection_url' or 'cloud_id'.")

    cache_key = _generate_cache_key(config)

    if use_cache and cache_key in _es_clients:
        cached_client = _es_clients[cache_key]
        try:
            if cached_client.ping():
                logger.debug(f"Returning Elasticsearch client from cache for key: {cache_key}")
                return cached_client
            else:
                logger.warning(f"Cached Elasticsearch client (key: {cache_key}) failed ping test. Creating a new one.")
                # Remove "dead" client from cache
                _es_clients.pop(cache_key, None)
        except ESConnectionError: # Explicit connection error
            logger.warning(f"Cached Elasticsearch client (key: {cache_key}) seems disconnected. Creating a new one.")
            _es_clients.pop(cache_key, None)
        except Exception as e: # Any other exception with the cached client
            logger.error(f"Unexpected error with cached Elasticsearch client (key: {cache_key}): {e}. Creating a new one.", exc_info=True)
            _es_clients.pop(cache_key, None)

    logger.debug(f"Creating new Elasticsearch client for config (cache key: {cache_key}).")
    try:
        client_params: Dict[str, Any] = {
            "request_timeout": config.get("request_timeout", 60), # Default request timeout
            "verify_certs": config.get("verify_certs", True), # Default to verify SSL certs
            "http_compress": config.get("http_compress", True), # Default to use HTTP compression
            "retry_on_timeout": config.get("retry_on_timeout", True), # Enable retries on timeout by default
            "max_retries": config.get("max_retries", 3), # Default number of retries
        }

        # Main connection setup (Cloud ID or URL)
        if config.get("cloud_id"):
            client_params["cloud_id"] = config["cloud_id"]
        elif config.get("es_connection_url"):
            # Supports multiple URLs if provided as a list or a comma-separated string
            hosts_input: Union[str, List[str]] = config["es_connection_url"]
            if isinstance(hosts_input, str):
                client_params["hosts"] = [h.strip() for h in hosts_input.split(',')]
            elif isinstance(hosts_input, list):
                client_params["hosts"] = hosts_input # Assumes list of strings
            else:
                raise ValueError("'es_connection_url' must be a string or a list of strings.")

        # Authentication: API Key takes precedence over Basic Auth
        if config.get("api_key"):
            client_params["api_key"] = config["api_key"] # Can be a string or (id, api_key) tuple
        elif config.get("es_user") is not None: # Allow empty user if password is also empty (rare case)
            client_params["basic_auth"] = (config["es_user"], config.get("es_password")) # Use basic_auth for ES8+

        # SSL settings (if verify_certs=True)
        if client_params["verify_certs"]:
            if config.get("ca_certs"):
                client_params["ca_certs"] = config["ca_certs"]
            if config.get("client_cert") and config.get("client_key"):
                client_params["client_cert"] = config["client_cert"]
                client_params["client_key"] = config["client_key"]
        else:
            # The InsecureRequestWarning is already handled globally in logging_config.py
            logger.warning(
                f"Elasticsearch client configured with verify_certs=False for "
                f"'{config.get('es_connection_url', config.get('cloud_id'))}'. "
                "This is insecure for production environments."
            )

        # Pass any other valid Elasticsearch constructor arguments directly
        # Filter to avoid passing keys that are not valid for the Elasticsearch constructor
        valid_es_constructor_args = {
            "ssl_assert_hostname", "ssl_assert_fingerprint", "ssl_version", "ssl_context",
            "sniffer_timeout", "sniff_on_start", "sniff_before_requests", "sniff_on_connection_fail",
            "transport_class", "node_class", "connection_class", "connections_per_node",
            "headers", "opaque_id",
            # Elasticsearch-py 8.x uses 'serializers' which expects a dict. 'serializer' is for older versions.
            # If you need to support custom serializers with ES8+, pass it via `serializers`
            # "serializers" 
        }
        extra_es_params = {k: v for k, v in config.items() if k in valid_es_constructor_args}
        client_params.update(extra_es_params)

        es_client = Elasticsearch(**client_params)

        if not es_client.ping():
            logger.error(f"Failed to connect (ping) to Elasticsearch cluster at "
                         f"'{config.get('es_connection_url', config.get('cloud_id'))}' after client creation.")
            return None # Do not cache a client that failed the initial ping

        logger.info(f"Elasticsearch client initialized successfully for "
                    f"'{config.get('es_connection_url', config.get('cloud_id'))}'.")

        if use_cache:
            _es_clients[cache_key] = es_client
            logger.debug(f"New Elasticsearch client cached with key: {cache_key}")

        return es_client

    except (ESConnectionError, AuthenticationException, AuthorizationException) as e:
        logger.error(f"Connection, authentication, or authorization error initializing Elasticsearch client for "
                     f"'{config.get('es_connection_url', config.get('cloud_id'))}': {e}", exc_info=False) # exc_info=False to avoid verbose stack for common connection issues
        return None
    except TypeError as e: # Error in arguments passed to Elasticsearch()
        logger.error(f"Type error initializing Elasticsearch client, possibly due to invalid configuration parameters: {e}", exc_info=True)
        raise ValueError(f"Invalid parameters for Elasticsearch client: {e}") from e # Re-raise to indicate config issue
    except Exception as e:
        logger.error(f"Unexpected error initializing Elasticsearch client for "
                     f"'{config.get('es_connection_url', config.get('cloud_id'))}': {e}", exc_info=True)
        return None


def close_es_client(config: Optional[Dict[str, Any]] = None, cache_key: Optional[str] = None, client_instance: Optional[Elasticsearch] = None):
    """
    Tenta fechar uma conexão de cliente Elasticsearch específica e removê-la do cache, se aplicável.
    Pode identificar o cliente pela configuração, chave de cache ou pela instância direta.

    Nota: O cliente Python do Elasticsearch gerencia um pool de conexões. Chamar `close()`
    no cliente (em versões mais recentes) geralmente fecha o 'transport' e limpa as conexões no pool.

    Args:
        config (Optional[Dict[str, Any]]): Dicionário de configuração usado para gerar a chave do cache.
        cache_key (Optional[str]): A chave de cache direta do cliente.
        client_instance (Optional[Elasticsearch]): A instância do cliente a ser fechada.
    """
    key_to_close = cache_key
    client_to_close: Optional[Elasticsearch] = client_instance
    log_identifier_parts = []

    if not key_to_close and config:
        key_to_close = _generate_cache_key(config)

    if key_to_close:
        log_identifier_parts.append(f"CacheKey: {key_to_close}")

    # If we have a key, try to get the client from the cache to ensure we are closing the right one
    # and to be able to remove it from the cache by name.
    if key_to_close and key_to_close in _es_clients:
        if not client_to_close: # If instance was not passed, get from cache
            client_to_close = _es_clients.get(key_to_close)
        # If instance was passed AND a key, and they don't match, log a warning
        elif client_to_close is not _es_clients.get(key_to_close):
             logger.warning(f"Provided client instance for closure does not match the cached instance for key '{key_to_close}'. "
                            "The cached client will be removed if the provided instance is closed successfully.")

    if client_instance and not key_to_close: # Closing a non-cached or directly provided instance
        log_identifier_parts.append(f"DirectInstance: {id(client_instance)}")


    log_identifier = "; ".join(log_identifier_parts) if log_identifier_parts else "UnidentifiedClient"

    if client_to_close:
        logger.info(f"Attempting to close Elasticsearch client ({log_identifier}).")
        try:
            # The close() method exists in recent versions of elasticsearch-py client (>=7.x or >=8.x)
            # For older versions, closure was more implicit or via transport.
            if hasattr(client_to_close, 'close') and callable(client_to_close.close):
                 client_to_close.close()
                 logger.info(f"Elasticsearch client closed successfully ({log_identifier}).")
            else:
                 # For older clients or if 'close' is not the expected method.
                 # Connections are pooled and typically managed by the transport layer.
                 es_version = getattr(Elasticsearch, '__version__', 'unknown') # type: ignore
                 if isinstance(es_version, tuple): # elasticsearch-py >= 8.x uses a tuple
                     es_version_str = ".".join(map(str, es_version))
                 else: # elasticsearch-py < 8.x used a string or did not have __version__
                     es_version_str = str(es_version)
                 logger.info(f"Elasticsearch client (version: {es_version_str}) may not have an explicit high-level 'close()' method, "
                               f"or relies on 'transport' closure. Connections are pooled. ({log_identifier})")

        except Exception as e:
            logger.error(f"Error closing Elasticsearch client ({log_identifier}): {e}", exc_info=True)
        finally:
            # Always remove from cache if the key was used to identify it
            if key_to_close and key_to_close in _es_clients:
                # Ensure we are removing the same client we attempted to close,
                # or if a different instance was provided, remove the one from cache by key anyway.
                if _es_clients.get(key_to_close) is client_to_close or client_instance is not None:
                    _es_clients.pop(key_to_close, None)
                    logger.debug(f"Client removed from cache for key: {key_to_close}.")
    else:
        logger.warning(f"Attempt to close non-existent or unidentified Elasticsearch client ({log_identifier}).")


def close_all_cached_es_clients():
    """Fecha todos os clientes Elasticsearch atualmente mantidos no cache e limpa o cache."""
    if not _es_clients:
        logger.info("No cached Elasticsearch clients to close.")
        return

    logger.info(f"Attempting to close all {len(_es_clients)} cached Elasticsearch clients.")
    # Iterate over a copy of the keys to allow modification of the _es_clients dictionary
    for key in list(_es_clients.keys()):
        # Pass the instance directly to ensure the correct object is closed
        client_instance_to_close = _es_clients.get(key)
        if client_instance_to_close:
            # close_es_client will handle removing it from the cache if key is provided
            close_es_client(cache_key=key, client_instance=client_instance_to_close)

    # Ensure the cache is empty after the process
    if _es_clients: # If anything was not removed by close_es_client (unlikely if logic is correct)
        logger.warning(f"ES client cache is not empty after close_all_cached_es_clients. "
                       f"Remaining clients: {len(_es_clients)}. Forcibly clearing.")
        _es_clients.clear()
    logger.info("All cached Elasticsearch clients have been processed for closure.")