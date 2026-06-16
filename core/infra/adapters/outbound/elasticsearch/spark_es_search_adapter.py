from typing import Any, Dict, List, Iterable
import logging
import socket
import itertools

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from core.application.ports.outbound.get_candidates_port import GetCandidatesPort
from core.application.ports.outbound.search_executor import SearchExecutor
from core.domain.models.linkage_specification import BlockingPhaseContext

from .client import get_es_client
from .query_builder import ElasticsearchQueryBuilder
from .response_parser import extract_hits_from_es_response

logger = logging.getLogger("Adapter: SparkESSearchAdapter")


def chunked_iterator(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            return
        yield chunk


class SparkESSearchAdapter(GetCandidatesPort):
    def __init__(self, index_name: str, es_config: Dict[str, Any], search_executor: SearchExecutor):        
        self.es_config = es_config
        self.index_name = index_name
        self.search_executor = search_executor

    @staticmethod
    def _normalize_candidate_record(
        hit: Dict[str, Any],
        phase_context: BlockingPhaseContext,
    ) -> Dict[str, Any]:
        hit_source = hit.get("source", {})
        normalized_record = {
            field_name: hit_source.get(field_name)
            for field_name in phase_context.target_fields.fetch_fields
        }
        normalized_record["_score"] = hit.get("score")
        return normalized_record

    def get_candidates(self, df_source: Any, phase_context: BlockingPhaseContext) -> Any:
        rules = phase_context.rules
        limit = phase_context.candidate_limit
        fetch_fields = phase_context.target_fields.fetch_fields
        config = self.es_config
        index = self.index_name

        # Resgata o tamanho limite do lote para a API _msearch (padrão 100)
        batch_size = config.get("msearch_batch_size", 100)

        def partition_search(spark_partition_iterator):            
            es_client = get_es_client(config)
            
            if es_client is None:
                raise RuntimeError("Elasticsearch client could not be initialized. Check connection.")
            
            query_builder = ElasticsearchQueryBuilder(
                phase_rules=rules,
                fetch_fields=fetch_fields,
                candidate_limit=limit,
                static_filter=phase_context.indexed_dataset_filter
            )

            # Transforma o iterador nativo do Spark em blocos/lotes sob demanda (Lazy)
            chunks = chunked_iterator(spark_partition_iterator, batch_size)

            for chunk in chunks:
                # Converte os Rows do Spark do lote atual para dicionários
                records = [record.asDict(recursive=True) for record in chunk]
                
                # Constrói as queries estruturadas do Elasticsearch para o lote inteiro
                queries = [query_builder.build_search_body_for_record(r) for r in records]

                # Executa o lote na rede (Delega para o MultiSearchExecutor ou SingleSearchExecutor)
                responses = self.search_executor.execute(es_client, index, queries)

                # Processa as respostas pareando os resultados um a um via zip
                for record_dict, response in zip(records, responses):
                    response_data = response.body if hasattr(response, "body") else response 
                    response_dict = dict(response_data) if not isinstance(response_data, dict) else response_data
                    
                    hits = extract_hits_from_es_response(
                        single_es_response=response_dict,
                        source_record_id_for_log=record_dict.get("id"),
                    )
                
                    for hit in hits:
                        candidate_record = self._normalize_candidate_record(
                            hit=hit,
                            phase_context=phase_context,
                        )
                        
                        yield Row(
                            source_record=Row(**record_dict),
                            candidate_record=Row(**candidate_record),
                        )
            
        # Aplicação da busca particionada no RDD do Spark
        rdd_candidates = df_source.rdd.mapPartitions(partition_search)

        candidate_schema = StructType([
            *[StructField(field, StringType(), True) for field in fetch_fields],
            StructField("_score", FloatType(), True),
        ])

        final_schema = StructType([
            StructField("source_record", df_source.schema, True),
            StructField("candidate_record", candidate_schema, True)
        ])

        return df_source.sparkSession.createDataFrame(rdd_candidates, schema=final_schema)