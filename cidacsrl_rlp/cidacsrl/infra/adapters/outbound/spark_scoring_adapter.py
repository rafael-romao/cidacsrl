from typing import Dict, Any
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StructField, StructType

from cidacsrl_rlp.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl_rlp.cidacsrl.domain.models.linkage_specification import BlockingPhaseContext
from cidacsrl_rlp.cidacsrl.domain.services.scoring_engine import calculate_pair_scores_and_similarities


class SparkScoringAdapter(ScoringPort):

    def _build_score_schema(self, phase_context: BlockingPhaseContext) -> StructType:
        fields = [StructField("match_score", FloatType(), nullable=True)]
        for rule in phase_context.rules:
            fields.append(StructField(f"sim_{rule.source_column}", FloatType(), nullable=True))
        return StructType(fields)

    def calculate_score(self, df_candidates: Any, phase_context: BlockingPhaseContext) -> Any:
        score_schema = self._build_score_schema(phase_context)
        rules = phase_context.rules
        threshold = phase_context.strong_match_score_threshold

        source_field_names = [
            field.name for field in df_candidates.schema["source_record"].dataType.fields
        ]

        @F.udf(returnType=score_schema)
        def compute_scores_udf(source_row: Any, candidate_row: Any) -> Dict[str, Any]:
            return calculate_pair_scores_and_similarities(
                source_row.asDict(recursive=True),
                candidate_row.asDict(recursive=True),
                rules,
            )
    
        df_scored = df_candidates.withColumn(
            "score_struct", 
            compute_scores_udf(F.col("source_record"), F.col("candidate_record"))
        )

        df_filtered = df_scored.filter(F.col("score_struct.match_score") >= threshold)

        source_fields = [
            F.col(f"source_record.{field_name}").alias(f"source_{field_name}") 
            for field_name in source_field_names
        ]

        candidate_fields = [
            F.col(f"candidate_record.{field_name}").alias(f"candidate_{field_name}") 
            for field_name in phase_context.target_fields.result_fields
        ]

        return df_filtered.select(
            *source_fields,
            *candidate_fields,
            "score_struct.*"
        )