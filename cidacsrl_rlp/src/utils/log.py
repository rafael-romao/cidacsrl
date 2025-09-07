import pandas as pd
from datetime import datetime
import os
import uuid
import pytz

def trace_execution(process_name: str, operation: str, caminho_csv: str, execution_id: str = None):
    """
    Registra um evento estilo CDC (Change Data Capture) para início/fim de um processo.

    Parâmetros:
    - process_name: Nome do processo
    - operation: 'START' ou 'END'
    - caminho_csv: Caminho do arquivo CSV de log
    """
    # TODO: melhorar esse útil e criar uma classe, onde podemos instanciar um objeto que pode ter os métodos obj.start() e obj.end() que já registra automaticamente.
    
    # Geração do novo estado baseado na operação
    if operation.upper() == 'START':
        old_state = None
        new_state = 'running'
    elif operation.upper() == 'END':
        old_state = 'running'
        new_state = 'completed'
    else:
        raise ValueError("Operação inválida. Use 'START' ou 'END'.")

    # Criar novo evento
    novo_evento = {
        "event_id": str(uuid.uuid4()),
        "process_name": process_name,
        "operation": operation.upper(),
        "timestamp": datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S"),
        "old_state": old_state,
        "new_state": new_state,
        "execution_id": execution_id, # optional
    }

    # Lê o histórico de log (caso já exista)
    if os.path.exists(caminho_csv):
        df = pd.concat(
            [
                pd.read_csv(caminho_csv),
                pd.DataFrame([novo_evento])
            ],
            ignore_index=True
        )
    else:
        df = pd.DataFrame([novo_evento])

    # Salvar no arquivo CSV
    df.to_csv(caminho_csv, index=False)
