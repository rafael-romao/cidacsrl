from cidacsrl_rlp.cidacsrl.application.ports.outbound.get_candidates_port import GetCandidatesPort
from cidacsrl_rlp.cidacsrl.domain.models.rules import BlockingPhase
from .client import get_es_client
from .query_builder import ElasticsearchQueryBuilder
from .response_parser import extract_hits_from_es_response
from typing import Any



class SparkESSearchAdapter(GetCandidatesPort):
    def __init__(self, index_name: str, es_url: str):        
        self.es_url = es_url
        self.index_name = index_name 

    def get_candidates(self, df_source: Any, phase_config: BlockingPhase) -> Any:
        rules = phase_config.rules
        limit = phase_config.candidate_limit
        fields = phase_config.target_fields
        url = self.es_url
        index = self.index_name

        def partition_search(partition):
            es_client = get_es_client(url)
            query_builder = ElasticsearchQueryBuilder(
                phase_rules=rules,
                target_fields=fields,
                candidate_limit=limit
            )
            for record in partition:
                record_dict = record.asDict()
                query_body = query_builder.build_search_body_for_record(record_dict)
                response = es_client.search(index=index, body=query_body)
                hits = extract_hits_from_es_response(
                    response,
                    source_record_id_for_log=record_dict.get("id"),
                )
            
                for hit in hits:
                    yield {"source_record": record_dict, "candidate_record": hit["source"]}
            
        rdd_candidates = df_source.rdd.mapPartitions(partition_search)
        return rdd_candidates.toDF()