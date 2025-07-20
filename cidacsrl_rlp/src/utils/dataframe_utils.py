# src/utils/dataframe_utils.py

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

logger = logging.getLogger(__name__)

def safely_unpersist(
    resource: Optional[Any],
    resource_name: str = "SparkResource",
    blocking: bool = False
) -> None:
    """
    Tenta despersistir um Spark DataFrame ou RDD de forma segura.

    Verifica se o recurso existe, se possui o atributo `is_cached`
    (para diferenciar de objetos `None` ou não-Spark), se está efetivamente em cache,
    e então tenta a operação `unpersist()`.

    Args:
        resource (Optional[Any]): O `DataFrame` ou `RDD` Spark a ser despersistido.
                                  Pode ser `None`.
        resource_name (str): Um nome descritivo para o recurso, usado em logs (padrão: "SparkResource").
        blocking (bool): Se a operação `unpersist` deve ser bloqueante (padrão: False).
    """
    if resource is None:
        logger.debug(f"Resource '{resource_name}' is None, nothing to do for unpersist.")
        return

    if not (isinstance(resource, DataFrame) or isinstance(resource, RDD)):
        logger.warning(f"Resource '{resource_name}' is not a Spark DataFrame or RDD "
                       f"(type: {type(resource).__name__}). Cannot unpersist in the standard way.")
        return

    # Check if the resource has the 'is_cached' attribute, which is typical for Spark DataFrames/RDDs.
    if hasattr(resource, 'is_cached'):
        try:
            if resource.is_cached:
                logger.debug(f"Attempting to unpersist resource '{resource_name}' (blocking={blocking})...")
                resource.unpersist(blocking=blocking)
                logger.info(f"Resource '{resource_name}' unpersisted successfully.")
            else:
                logger.debug(f"Resource '{resource_name}' is not cached (is_cached=False), "
                               f"no unpersist action needed.")
        except Exception as e:
            logger.warning(f"Error attempting to unpersist resource '{resource_name}': {e}", exc_info=True)
    else:
        # This path should ideally not be reached for valid Spark DataFrames/RDDs.
        logger.warning(f"Resource '{resource_name}' (type: {type(resource).__name__}) "
                       f"does not have the 'is_cached' attribute. Cannot verify or unpersist.")