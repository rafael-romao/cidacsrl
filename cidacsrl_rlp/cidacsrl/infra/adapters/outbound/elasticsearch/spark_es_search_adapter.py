from typing import Any, Dict

from pyspark.sql import Row
import logging
import socket

from cidacsrl_rlp.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext

from pyspark.sql.types import StructType, StructField, StringType

from .client import get_es_client
from .query_builder import ElasticsearchQueryBuilder
from .response_parser import extract_hits_from_es_response

logger = logging.getLogger(__name__)



class SparkESSearchAdapter(GetCandidatesPort):
    def __init__(self, index_name: str, es_config: Dict[str, Any]):        
        self.es_config = es_config
        self.index_name = index_name 

    @staticmethod
    def _normalize_candidate_record(
        hit_source: Dict[str, Any],
        phase_context: BlockingPhaseContext,
    ) -> Dict[str, Any]:
        hit_source = hit_source or {}
        return {
            field_name: hit_source.get(field_name)
            for field_name in phase_context.target_fields.fetch_fields
        }

    def get_candidates(self, df_source: Any, phase_context: BlockingPhaseContext) -> Any:
        rules = phase_context.rules
        limit = phase_context.candidate_limit
        fetch_fields = phase_context.target_fields.fetch_fields
        config = self.es_config
        index = self.index_name

        def partition_search(partition):
            print(f"Partition running on host: {socket.gethostname()}")
            print(f"ES config: {config}")
            es_client = get_es_client(config)
            if es_client is None:
                print("Failed to create ES client in partition.")
                raise RuntimeError("Elasticsearch client could not be initialized. Check connection and configuration.")       
            es_client = get_es_client(config)
            query_builder = ElasticsearchQueryBuilder(
                phase_rules=rules,
                fetch_fields=fetch_fields,
                candidate_limit=limit
            )
            for record in partition:
                if es_client is None:
                    raise RuntimeError("Elasticsearch client could not be initialized. Check connection and configuration.")
                record_dict = record.asDict(recursive=True)
                query_body = query_builder.build_search_body_for_record(record_dict)
                response = es_client.search(index=index, body=query_body)
                response_data = response.body if hasattr(response, "body") else response 
                response_dict = dict(response_data) if not isinstance(response_data, dict) else response_data
                hits = extract_hits_from_es_response(
                    single_es_response=response_dict,
                    source_record_id_for_log=record_dict.get("id"),
                )
            
                for hit in hits:
                    candidate_record = SparkESSearchAdapter._normalize_candidate_record(
                        hit.get("source"),
                        phase_context,
                    )
                    yield Row(
                        source_record=Row(**record_dict),
                        candidate_record=Row(**candidate_record),
                    )
            
        rdd_candidates = df_source.rdd.mapPartitions(partition_search)

        candidate_schema = StructType([
            StructField(field, StringType(), True) 
            for field in fetch_fields
        ])

        final_schema = StructType([
            StructField("source_record", df_source.schema, True),
            StructField("candidate_record", candidate_schema, True)
        ])

        return df_source.sparkSession.createDataFrame(rdd_candidates, schema=final_schema)