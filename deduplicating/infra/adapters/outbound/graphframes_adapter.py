import logging
from typing import Any

import pyspark.sql.functions as F
from graphframes import GraphFrame

from cidacsrl.ports.deduplication.graph_processing_port import GraphProcessingPort

logger = logging.getLogger("Adapter: GraphFrames")


class GraphFramesAdapter(GraphProcessingPort):
    """Implementa GraphProcessingPort usando GraphFrames (connectedComponents).

    O GraphFrame e a coluna interna 'component' são detalhes de implementação
    — o port devolve sempre (id, cluster_id) para o use case.
    """

    def find_clusters(
        self,
        df_pairs: Any,
        id_source_column: str,
        id_target_column: str,
    ) -> Any:
        df_src_ids = df_pairs.select(F.col(id_source_column).alias("id"))
        df_dst_ids = df_pairs.select(F.col(id_target_column).alias("id"))
        df_vertices = df_src_ids.union(df_dst_ids).distinct()

        df_edges = df_pairs.select(
            F.col(id_source_column).alias("src"),
            F.col(id_target_column).alias("dst"),
        )

        logger.info("Vértices: %s | Arestas: %s", df_vertices.count(), df_edges.count())

        graph = GraphFrame(df_vertices, df_edges)
        df_components = graph.connectedComponents()

        distinct_clusters = df_components.select("component").distinct().count()
        logger.info("Componentes conectados encontrados: %s", distinct_clusters)

        return df_components.withColumnRenamed("component", "cluster_id")
