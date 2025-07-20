# src/es/mapping_models.py

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Literal

logger = logging.getLogger(__name__)

@dataclass
class ESColumnDefinition:
    """
    Define as propriedades de mapeamento para uma única coluna/campo em um índice Elasticsearch.

    Atributos:
        name (str): Nome da coluna/campo.
        type (Literal["string", "integer", "long", "float", "double", "date", "boolean", "text", "keyword"]):
                      O tipo de dado Elasticsearch principal para o campo.
                      "string" é tratado como "text" a menos que `index_as` especifique o contrário.
        index_as (Optional[Literal["keyword", "text", "both"]]): Específico para `type="string"` ou `type="text"`.
                      Determina como o campo de string será indexado.
                      - "text": Para busca full-text (padrão para "string"/"text").
                      - "keyword": Para correspondência exata, ordenação e agregações.
                      - "both": Indexa como "text" e adiciona um subcampo ".keyword" do tipo "keyword".
        format (Optional[str]): Formato para campos do tipo "date" (ex: "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis").
        analyzer (Optional[str]): Analyzer a ser usado para campos do tipo "text".
        ignore_above (Optional[int]): Para campos "keyword", especifica o comprimento máximo de string a ser indexado.
                                       Strings mais longas não serão indexadas nem armazenadas (default: 256 se `index_as` for "both").
    """
    name: str
    type: Literal["string", "integer", "long", "float", "double", "date", "boolean", "text", "keyword"]
    index_as: Optional[Literal["keyword", "text", "both"]] = None
    format: Optional[str] = None
    analyzer: Optional[str] = None
    ignore_above: Optional[int] = None # Default applied in build_mapping_properties

    def __post_init__(self):
        if self.type == "string": # Treat "string" as an alias for "text" for mapping purposes
            self.type = "text"

        if self.index_as and self.type not in ["text", "keyword"]:
            logger.warning(f"Field '{self.name}': 'index_as' is specified but type is '{self.type}'. "
                           f"'index_as' is primarily for 'text' or 'keyword' types.")
        if self.format and self.type != "date":
            logger.warning(f"Field '{self.name}': 'format' is specified but type is '{self.type}'. "
                           f"'format' is for 'date' types.")
        if self.analyzer and self.type != "text":
            logger.warning(f"Field '{self.name}': 'analyzer' is specified but type is '{self.type}'. "
                           f"'analyzer' is for 'text' types.")
        if self.ignore_above and self.type != "keyword" and not (self.type == "text" and self.index_as == "both"):
             logger.warning(f"Field '{self.name}': 'ignore_above' is specified but type is not 'keyword' "
                            f"(or 'text' with index_as='both'). It might not have an effect.")


@dataclass
class ESIndexSettings:
    """
    Define as configurações (settings) para um índice Elasticsearch.

    Atributos:
        number_of_shards (int): Número de shards primários para o índice (default: 1).
        number_of_replicas (int): Número de réplicas para cada shard primário (default: 1).
        refresh_interval (Optional[str]): Intervalo de refresh do índice (ex: "1s", "-1" para desabilitar) (default: None, usa o padrão do ES).
        # Outras configurações comuns do índice podem ser adicionadas aqui
        # Ex: analysis (para custom analyzers, tokenizers, etc.)
        analysis: Optional[Dict[str, Any]] = None
    """
    number_of_shards: int = 1
    number_of_replicas: int = 1
    refresh_interval: Optional[str] = None
    analysis: Optional[Dict[str, Any]] = None


@dataclass
class ESIndexDefinition:
    """
    Define a estrutura completa de um índice Elasticsearch, incluindo nome, settings e mapeamento de colunas.

    Atributos:
        name (str): O nome do índice Elasticsearch.
        settings (ESIndexSettings): As configurações do índice.
        columns (List[ESColumnDefinition]): A lista de definições de coluna para o mapeamento do índice.
    """
    name: str
    settings: ESIndexSettings
    columns: List[ESColumnDefinition]

    def _build_mapping_properties(self) -> Dict[str, Any]:
        """
        Constrói a seção 'properties' do mapeamento Elasticsearch a partir das definições de coluna.
        Este é um método auxiliar para `build_index_creation_body`.

        Returns:
            Dict[str, Any]: O dicionário de propriedades para o mapeamento do ES.
        """
        mapping_properties: Dict[str, Any] = {}
        for col_def in self.columns:
            col_mapping: Dict[str, Any] = {}

            # Determine base ES type
            if col_def.type == "text" or (col_def.type == "keyword" and col_def.index_as == "text"):
                col_mapping["type"] = "text"
                if col_def.analyzer:
                    col_mapping["analyzer"] = col_def.analyzer
            elif col_def.type == "keyword" or (col_def.type == "text" and col_def.index_as == "keyword"):
                col_mapping["type"] = "keyword"
                if col_def.ignore_above is not None:
                    col_mapping["ignore_above"] = col_def.ignore_above
            elif col_def.type in ["integer", "long", "float", "double", "date", "boolean"]:
                col_mapping["type"] = col_def.type
                if col_def.type == "date" and col_def.format:
                    col_mapping["format"] = col_def.format
            else:
                logger.error(f"Unsupported column base type '{col_def.type}' for column '{col_def.name}'. Skipping.")
                continue # Should be caught by Literal, but as safeguard

            # Handle 'index_as="both"' for text types (creates a .keyword sub-field)
            if col_def.type == "text" and col_def.index_as == "both":
                col_mapping["type"] = "text" # Main field is text
                if col_def.analyzer:
                    col_mapping["analyzer"] = col_def.analyzer

                keyword_subfield: Dict[str, Any] = {"type": "keyword"}
                if col_def.ignore_above is not None:
                    keyword_subfield["ignore_above"] = col_def.ignore_above
                else: # Default ignore_above for .keyword subfields if 'both'
                    keyword_subfield["ignore_above"] = 256

                col_mapping["fields"] = {"keyword": keyword_subfield}

            mapping_properties[col_def.name] = col_mapping
        return mapping_properties

    def build_index_creation_body(self) -> Dict[str, Any]:
        """
        Constrói o corpo completo (settings e mappings) para a requisição de criação do índice no Elasticsearch.

        Returns:
            Dict[str, Any]: O dicionário pronto para ser usado como corpo na API de criação de índice do ES.
        """
        index_settings_dict: Dict[str, Any] = {
            "number_of_shards": self.settings.number_of_shards,
            "number_of_replicas": self.settings.number_of_replicas,
        }
        if self.settings.refresh_interval:
            index_settings_dict["refresh_interval"] = self.settings.refresh_interval
        if self.settings.analysis:
            index_settings_dict["analysis"] = self.settings.analysis
        # Can add more settings from self.settings here

        full_body = {
            "settings": {"index": index_settings_dict},
            "mappings": {"properties": self._build_mapping_properties()},
        }
        logger.debug(f"Built index creation body for '{self.name}': {full_body}")
        return full_body