import json
from typing import Dict, Any
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from cidacsrl.ports.linkage.scoring_port import ScoringPort
from cidacsrl.domain.linkage.linkage_specification import BlockingPhaseContext
from cidacsrl.domain.linkage.scoring_engine import calculate_pair_scores_and_similarities


class SparkScoringAdapter(ScoringPort):

    def _build_score_schema(self, phase_context: BlockingPhaseContext, debug: bool = False) -> StructType:
        fields = [StructField("match_score", FloatType(), nullable=True)]
        for rule in phase_context.rules:
            fields.append(StructField(f"sim_{rule.source_column}", FloatType(), nullable=True))
        if debug:
            fields.append(StructField("score_debug_json", StringType(), nullable=True))
        return StructType(fields)

    def calculate_score(self, df_candidates: Any, phase_context: BlockingPhaseContext, debug: bool = False) -> Any:
        score_schema = self._build_score_schema(phase_context, debug=debug)
        rules = phase_context.rules
        threshold = phase_context.strong_match_score_threshold

        source_field_names = [
            field.name for field in df_candidates.schema["source_record"].dataType.fields
        ]

        @F.udf(returnType=score_schema)
        def compute_scores_udf(source_row: Any, candidate_row: Any) -> Dict[str, Any]:
            score_result = calculate_pair_scores_and_similarities(
                source_row.asDict(recursive=True) if hasattr(source_row, "asDict") else source_row,
                candidate_row.asDict(recursive=True) if hasattr(candidate_row, "asDict") else candidate_row,
                rules,
                debug=debug,
            )

            if debug:
                score_result["score_debug_json"] = json.dumps(score_result.pop("_debug", {}))

            return score_result
    
        df_scored = df_candidates.withColumn(
            "score_struct", 
            compute_scores_udf(F.col("source_record"), F.col("candidate_record"))
        )        

        source_fields = [
            F.col(f"source_record.{field_name}").alias(f"source_{field_name}") 
            for field_name in source_field_names
        ]

        candidate_fields = [
            F.col(f"candidate_record.{field_name}").alias(f"candidate_{field_name}") 
            for field_name in phase_context.target_fields.result_fields
        ]

        return df_scored.select(
            *source_fields,
            *candidate_fields,
            "score_struct.*",
            F.col("candidate_record._score").alias("es_score")
        )