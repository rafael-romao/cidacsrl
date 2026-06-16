import logging
import sys

def configure_logging(level: int = logging.INFO) -> None:
    """
    Configura o formato global de logs para a aplicação.
    Aplica formatação compacta e alinhada para facilitar a leitura no console.
    """
    # Remove configurações prévias se o método for chamado múltiplas vezes
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_format = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)]
    )