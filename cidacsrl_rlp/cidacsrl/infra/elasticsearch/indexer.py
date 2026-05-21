import logging
import time
from typing import Dict, Any

from elasticsearch import Elasticsearch

# Importar a função do client.py
from cidacsrl_rlp.cidacsrl.infra.elasticsearch.client import get_es_client

logger = logging.getLogger(__name__)


class ElasticsearchIndexer:
    def __init__(self, es_config: Dict[str, Any], index_config: Dict[str, Any]):
        """
        Initialize the ElasticsearchIndexer.

        Args:
            es_config: Dicionário com detalhes da conexão Elasticsearch.
            index_config: Dicionário com configurações do índice.
        """
        self.es_config = es_config
        self.index_config = index_config

        # Get Elasticsearch connection details from index_config first if available
        idx_cfg = self.index_config.get("index_config", {})
        self.es_index = idx_cfg.get("name")
        self.replicas = idx_cfg.get("number_of_replicas", 1)
        self.shards = idx_cfg.get("number_of_shards", 1)

        # Get connection details primarily from es_config
        self.es_nodes = self.es_config.get("es_connection_url")
        self.es_user = self.es_config.get("es_user")
        self.es_password = self.es_config.get("es_password")

        self.columns = self.index_config.get("columns")

        if not self.es_index:
            raise ValueError("Nome do índice ('index_config.name') não encontrado na configuração do índice.")
        if not self.es_nodes:
             raise ValueError("URL de conexão ('es_connection_url') não encontrada na configuração do ES.")
        if not self.columns:
             raise ValueError("Configuração de colunas ('columns') não encontrada na configuração do índice.")


        logger.info(f"Index name: {self.es_index}")
        logger.info(f"Elasticsearch nodes: {self.es_nodes}")

    def _create_mapping(self, column_mapping):
        """
        Creates an Elasticsearch mapping and settings based on the column mapping.

        The `index_as` parameter for string fields can be:
            - 'keyword': Indexes for exact matching.
            - 'text': Indexes for full-text search.
            - 'both': Indexes for both exact matching and full-text search.
        """
        mapping_body = {
            "settings": {
                "index": {
                    "number_of_shards": self.shards,
                    "number_of_replicas": self.replicas,
                }
            },
            "mappings": {"properties": {}},
        }
        for column in column_mapping:
            col_name = column["name"]
            col_type = column["type"]
            if col_type is None:
                raise ValueError(f"Column type is missing for column: {column['name']}")
            elif col_type == "string":
                index_as = column.get("index_as", "text")
                if index_as == "keyword":
                    mapping_body["mappings"]["properties"][col_name] = {
                        "type": "keyword"
                    }
                elif index_as == "both":
                    mapping_body["mappings"]["properties"][col_name] = {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}},
                    }
                elif index_as == "text":
                    mapping_body["mappings"]["properties"][col_name] = {"type": "text"}
                else:
                    raise ValueError(f"Invalid index_as value: {index_as}")
            elif col_type == "integer":
                mapping_body["mappings"]["properties"][col_name] = {"type": "integer"}
            elif col_type == "double":
                mapping_body["mappings"]["properties"][col_name] = {"type": "double"}
            elif col_type == "date":
                mapping_body["mappings"]["properties"][col_name] = {"type": "date"}
            elif col_type == "boolean":
                mapping_body["mappings"]["properties"][col_name] = {"type": "boolean"}
            else:
                raise ValueError(f"Unsupported column type: {col_type}")
        return mapping_body

    def create_index(self, df):
        """
        Creates an index on Elasticsearch cluster.

        Args:
            df: The DataFrame to index.
        """
        try:
            # Get the column mapping
            column_mapping = self.columns

            # Create the Elasticsearch mapping
            mapping = self._create_mapping(column_mapping)

            # Write the data to Elasticsearch using elasticsearch-hadoop
            es_dict_conf = {
                "es.nodes": self.es_nodes,
                "es.resource": self.es_index,
                "es.nodes.wan.only": "true",
                "es.net.ssl.cert.allow.self.signed": "false",
                "es.net.http.auth.user": self.es_user,
                "es.net.http.auth.pass": self.es_password,
            }

            # Create the index if it doesn't exist
            es_auth = None
            if self.es_user and self.es_password:
                es_auth = (self.es_user, self.es_password)

            es = Elasticsearch(
                self.es_nodes,
                basic_auth=es_auth,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=90,
            )

            if not es.indices.exists(index=self.es_index):
                logger.info(f"Creating index '{self.es_index}' with mapping: {mapping}")
                es.indices.create(index=self.es_index, body=mapping)

            # Save the dataframe on Elasticsearch
            index_start_time = time.time()
            df.write.format("org.elasticsearch.spark.sql").options(**es_dict_conf).mode(
                "overwrite"
            ).save()
            index_duration_seconds = time.time() - index_start_time
            index_duration_minutes = int(index_duration_seconds // 60)
            index_duration_seconds = int(index_duration_seconds % 60)
            logger.info(
                f"Data indexed into Elasticsearch index '{self.es_index}' in {index_duration_minutes} minutes and {index_duration_seconds} seconds."
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Error indexing data into Elasticsearch: {e}")
            raise
