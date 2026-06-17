import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Union

from deduplicating.infra.configs.models.deduplicate_workflow_config import DeduplicateWorkflowConfig

logger = logging.getLogger("Loader: Deduplication Config")


def _load_yaml(file_path: Union[str, Path]) -> Dict[str, Any]:
    path = Path(file_path).resolve()

    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: '{path}'.")
    if not path.is_file():
        raise ValueError(f"O caminho '{path}' não é um arquivo válido.")
    if path.suffix.lower() not in (".yaml", ".yml"):
        raise ValueError(f"O arquivo '{path.name}' deve ser um YAML válido (.yml ou .yaml).")

    with open(path, "r", encoding="utf-8") as f:
        try:
            content = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Erro ao parsear o YAML '{path.name}': {e}") from e

    if content is None:
        return {}
    if not isinstance(content, dict):
        raise ValueError(f"O conteúdo do YAML '{path.name}' deve ser um dicionário.")

    return content


def load_deduplicate_workflow_config(path: Union[str, Path]) -> DeduplicateWorkflowConfig:
    """Carrega e valida o arquivo de configuração do workflow de deduplicação."""
    logger.info(f"Carregando configuração do workflow: '{path}'")
    data = _load_yaml(path)
    config = DeduplicateWorkflowConfig.from_dict(data)
    logger.info(
        f"Configuração carregada — "
        f"source='{config.storage.source_path}', "
        f"output='{config.storage.output_path}'."
    )
    return config
