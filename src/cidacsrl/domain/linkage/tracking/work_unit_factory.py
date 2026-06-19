from typing import List, Dict, Any, Optional
from cidacsrl.domain.linkage.tracking.work_unit import WorkUnitMetadata

class WorkUnitFactory:
    @staticmethod
    def create_execution_scope(
        partition_column: Optional[str], 
        filter_partitions: List[Any]
    ) -> List[WorkUnitMetadata]:
        if not partition_column:
            return [WorkUnitMetadata(unit_id="global", filters={})]

        if not filter_partitions:
            raise ValueError(
                f"Coluna de partição '{partition_column}' foi configurada, "
                f"mas nenhuma partição específica foi informada em 'filter_partitions'."
            )

        work_units = []
        for partition_value in filter_partitions:
            sanitized_value = str(partition_value).strip().replace(" ", "_")
            unit_id = f"{partition_column}_{sanitized_value}"
            
            work_units.append(
                WorkUnitMetadata(
                    unit_id=unit_id,
                    filters={partition_column: partition_value}
                )
            )
            
        return work_units