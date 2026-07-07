import logging
from typing import Any, Dict
from urllib.parse import urlparse

from pyspark.sql import DataFrame

from cidacsrl.adapters.outbound.elasticsearch.client import get_es_client
from cidacsrl.domain.indexing.indexing_specification import (
    DatasetIndexingSpecification,
)
from cidacsrl.ports.indexing.data_indexing_port import DataIndexingPort

logger = logging.getLogger("Adapter: SparkESIndexingAdapter")

class SparkESIndexingAdapter(DataIndexingPort):
    def __init__(self, es_config: Dict[str, Any]):
        self.es_config = es_config

    @staticmethod
    def _build_column_property(col) -> Dict[str, Any]:
        """Traduz uma IndexColumnConfig em uma propriedade de mapping do Elasticsearch.

        Honra:
          - index_as: 'both' (text + subcampo .keyword), 'keyword' (só keyword) ou
            'text'/None (só text);
          - format em campos 'date';
          - analyzer em campos 'text';
          - ignore_above em campos 'keyword' (e no subcampo .keyword de 'both',
            com default 256).
        """
        # Campo text puro OU text a ser indexado também/como full-text.
        if col.type == "text" and col.index_as != "keyword":
            prop: Dict[str, Any] = {"type": "text"}
            if col.analyzer:
                prop["analyzer"] = col.analyzer
            if col.index_as == "both":
                keyword_subfield: Dict[str, Any] = {"type": "keyword"}
                keyword_subfield["ignore_above"] = (
                    col.ignore_above if col.ignore_above is not None else 256
                )
                prop["fields"] = {"keyword": keyword_subfield}
            return prop

        # Campo keyword puro (type keyword, ou text com index_as='keyword').
        if col.type == "keyword" or (col.type == "text" and col.index_as == "keyword"):
            prop = {"type": "keyword"}
            if col.ignore_above is not None:
                prop["ignore_above"] = col.ignore_above
            return prop

        # Campo date com formato opcional.
        if col.type == "date":
            prop = {"type": "date"}
            if col.format:
                prop["format"] = col.format
            return prop

        # Demais tipos escalares (integer, long, float, double, boolean, ...).
        return {"type": col.type}

    def _build_es_mapping_payload(self, spec: DatasetIndexingSpecification) -> Dict[str, Any]:
        properties = {
            col.name: self._build_column_property(col)
            for col in spec.index_columns
        }

        index_settings: Dict[str, Any] = {
            "number_of_shards": spec.index_config.number_of_shards,
            "number_of_replicas": spec.index_config.number_of_replicas,
            "refresh_interval": spec.index_config.refresh_interval,
        }
        if spec.index_config.analysis:
            index_settings["analysis"] = spec.index_config.analysis

        return {
            "settings": {"index": index_settings},
            "mappings": {"properties": properties},
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

    def _build_es_write_options(self, id_field: str | None) -> Dict[str, Any]:
        es_url = self.es_config.get("es_connection_url", "http://localhost:9200")
        parsed = urlparse(es_url)
        es_options = {
            "es.nodes": parsed.hostname,
            "es.port": str(parsed.port or 9200),
            "es.nodes.wan.only": str(self.es_config.get("wan_only", True)).lower(),
        }

        if parsed.scheme == "https":
            es_options["es.net.ssl"] = "true"

        if self.es_config.get("verify_certs", True) is False:
            es_options["es.net.ssl.cert.allow.self.signed"] = "true"

        if id_field:
            es_options["es.write.operation"] = "upsert"
            es_options["es.mapping.id"] = id_field
            logger.info(f"Indexing with upsert operation using '{id_field}' as document ID.")
        else:
            es_options["es.write.operation"] = "index"

        if self.es_config.get("es_user") is not None:
            es_options["es.net.http.auth.user"] = self.es_config["es_user"]
            es_options["es.net.http.auth.pass"] = self.es_config.get("es_password")

        # Opções cruas do conector (prefixo "es.") têm precedência sobre os
        # defaults calculados acima — ex: "es.net.ssl.truststore.location" para mTLS.
        es_options.update({k: v for k, v in self.es_config.items() if k.startswith("es.")})

        return es_options

    def index_dataframe(self, df: DataFrame, spec: DatasetIndexingSpecification) -> None:
        index_name = spec.index_config.name
        id_field = spec.source_config.id_field if spec.index_config.id_from_source else None
        es_options = self._build_es_write_options(id_field)

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