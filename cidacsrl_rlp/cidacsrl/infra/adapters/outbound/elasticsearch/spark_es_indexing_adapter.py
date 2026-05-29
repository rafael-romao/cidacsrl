import logging
from typing import Dict, Any
from pyspark.sql import DataFrame
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_indexing_port import DataIndexingPort
from cidacsrl_rlp.cidacsrl.domain.models.indexing_specification import DatasetIndexingSpecification
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.client import get_es_client

logger = logging.getLogger(__name__)

class SparkESIndexingAdapter(DataIndexingPort):
    def __init__(self, es_config: Dict[str, Any]):
        self.es_config = es_config

    def _build_es_mapping_payload(self, spec: DatasetIndexingSpecification) -> Dict[str, Any]:
        properties = {}
        for col in spec.columns:
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

    def ensure_index_with_mapping(self, index_name: str, spec: DatasetIndexingSpecification) -> None:
        es_client = get_es_client(self.es_config, use_cache=False)
        
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' not found in ES. Creating mapping...")
            mapping_payload = self._build_es_mapping_payload(spec)
            es_client.indices.create(index=index_name, body=mapping_payload)
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.info(f"Index '{index_name}' already exists. Spark will append the data.")

    def index_dataframe(self, df: DataFrame, index_name: str, id_field: str) -> None:
        es_options = {
            "es.nodes": self.es_config.get("host", "localhost"),
            "es.port": str(self.es_config.get("port", 9200)),
            "es.nodes.wan.only": str(self.es_config.get("wan_only", True)).lower(),
            "es.mapping.id": id_field,
            "es.write.operation": "index"
        }
        
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