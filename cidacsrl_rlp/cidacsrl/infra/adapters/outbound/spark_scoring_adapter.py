from dataclasses import asdict
from typing import Dict, Any
from cidacsrl_rlp.cidacsrl.application.ports.outbound.scoring_port import ScoringPort
from cidacsrl_rlp.cidacsrl.domain.models.rules import BlockingPhase
from cidacsrl_rlp.cidacsrl.domain.services.scoring_engine import calculate_pair_scores_and_similarities
from pyspark.sql.types import StructType, StructField, FloatType
import pyspark.sql.functions as F


class SparkScoringAdapter(ScoringPort):

    def _build_score_schema(self, phase: BlockingPhase) -> StructType:
        fields = [StructField("match_score", FloatType(), nullable=True)]
        rules = phase.rules
        for rule in rules:
            sim_col_name = f"sim_{rule.source_column}"
            fields.append(StructField(sim_col_name, FloatType(), nullable=True))
        return StructType(fields)

    def calculate_score(self, df_candidates: Any, phase: BlockingPhase) -> Any:
        score_schema = self._build_score_schema(phase)
        rules = [asdict(rule) for rule in phase.rules]
        threshold = phase.strong_match_score_threshold

        @F.udf(returnType=score_schema)
        def compute_scores_udf(source_row: Any, candidate_row: Any) -> Dict[str, Any]:
            return calculate_pair_scores_and_similarities(
                source_row.asDict(recursive=True), 
                candidate_row.asDict(recursive=True), 
                rules,
                {"_candidate_prefix": ""}
            )
    
        df_scored = df_candidates.withColumn(
            "score_struct", 
            compute_scores_udf(F.col("source_record"), F.col("candidate_record")))

        df_filtered = df_scored.filter(F.col("score_struct.match_score") >= threshold)

        source_fields = [
            F.col(f"source_record.{f.name}").alias(f"source_{f.name}") 
            for f in df_filtered.schema["source_record"].dataType.fields
        ]

        candidate_fields = [
            F.col(f"candidate_record.{f.name}").alias(f"candidate_{f.name}") 
            for f in df_filtered.schema["candidate_record"].dataType.fields
        ]

        df_final = df_filtered.select(
            *source_fields,
            *candidate_fields,
            "score_struct.*"
        )

        return df_final      