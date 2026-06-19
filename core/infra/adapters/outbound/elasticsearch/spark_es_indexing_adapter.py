import logging
from typing import Dict, Any
from pyspark.sql import DataFrame
from cidacsrl.ports.indexing.data_indexing_port import DataIndexingPort
from cidacsrl.domain.indexing.indexing_specification import DatasetIndexingSpecification
from core.infra.adapters.outbound.elasticsearch.client import get_es_client
from urllib.parse import urlparse

logger = logging.getLogger("Adapter: SparkESIndexingAdapter")

class SparkESIndexingAdapter(DataIndexingPort):
    def __init__(self, es_config: Dict[str, Any]):
        self.es_config = es_config

    def _build_es_mapping_payload(self, spec: DatasetIndexingSpecification) -> Dict[str, Any]:
        properties = {}
        for col in spec.index_columns:
            if col.type == "text" and col.index_as == "both":
                properties[col.name] = {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                }
            else:
                properties[col.name] = {"type": col.type}

        return {
            "settings": {
                "index": {
                    "number_of_shards": spec.index_config.number_of_shards,
                    "number_of_replicas": spec.index_config.number_of_replicas,
                    "refresh_interval": spec.index_config.refresh_interval
                }
            },
            "mappings": {
                "properties": properties
            }
        }

    def ensure_index_with_mapping(self, spec: DatasetIndexingSpecification) -> None:
        es_client = get_es_client(self.es_config, use_cache=False)
        index_name = spec.index_config.name
        
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' not found in ES. Creating mapping...")
            mapping_payload = self._build_es_mapping_payload(spec)
            es_client.indices.create(index=index_name, body=mapping_payload)
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.info(f"Index '{index_name}' already exists. Spark will append the data.")

    def index_dataframe(self, df: DataFrame, spec: DatasetIndexingSpecification) -> None:
        index_name = spec.index_config.name
        id_field = spec.source_config.id_field if spec.index_config.id_from_source else None
        es_url = self.es_config.get("es_connection_url", "http://localhost:9200")
        parsed = urlparse(es_url)
        es_options = {
            "es.nodes": parsed.hostname,
            "es.port": str(parsed.port or 9200),
            "es.nodes.wan.only": "true"
        }

        if id_field:
            es_options["es.write.operation"] = "upsert"
            es_options["es.mapping.id"] = id_field
            logger.info(f"Indexing with upsert operation using '{id_field}' as document ID.")
        else:
            es_options["es.write.operation"] = "index"

        if "username" in self.es_config and "password" in self.es_config:
            es_options["es.net.http.auth.user"] = self.es_config["username"]
            es_options["es.net.http.auth.pass"] = self.es_config["password"]

        try:
            df.write \
                .format("es") \
                .options(**es_options) \
                .mode("append") \
                .save(index_name)
            logger.info(f"Bulk ingestion completed successfully for index '{index_name}'.")
        except Exception as e:
            logger.error(f"Error during Spark write operation for index '{index_name}': {e}")
            raise e