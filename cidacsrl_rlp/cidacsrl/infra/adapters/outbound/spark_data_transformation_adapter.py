from pyspark.sql import DataFrame
from cidacsrl_rlp.cidacsrl.application.ports.outbound.data_transformation_port import DataTransformationPort

class SparkDataTransformationAdapter(DataTransformationPort):
    """Operações puramente em memória, não exige caminhos ou configurações de disco."""
    def exclude_records(self, primary_dataset: DataFrame, records_to_exclude: DataFrame, join_key: str) -> DataFrame:
        join_condition = primary_dataset[join_key] == records_to_exclude[f"source_{join_key}"]
        return primary_dataset.join(records_to_exclude, on=join_condition, how="left_anti")