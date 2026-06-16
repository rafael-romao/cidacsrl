import os
import random
import pandas as pd
from faker import Faker
from pathlib import Path
from core.infra.configs.logging_config import configure_logging

configure_logging()
logger = logging.getLogger("Script::DataGenerator")


# Configurações iniciais
NUM_RECORDS_TARGET = 10000  # Tamanho do índice do ES (Óbitos)
NUM_RECORDS_SOURCE = 5000  # Tamanho da tabela a ser linkada (Acidentes)
OVERLAP_RATIO = 0.70        # 70% dos acidentados estarão na base de óbitos
UFS_VALIDAS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

fake = Faker('pt_BR')

def get_input_path() -> Path:
    """Retorna o diretório tests/data/input conforme a arquitetura do runner."""
    return Path(__file__).resolve().parents[1] / "data" / "input"

def introduce_noise(text: str) -> str:
    """Aplica ruídos ortográficos para testar a similaridade Jaro-Winkler."""
    if not text or random.random() > 0.6:  # 60% de chance de manter o texto intacto
        return text
    
    idx = random.randint(0, len(text) - 2)
    action = random.choice(['swap', 'delete'])
    
    if action == 'swap':
        # Inverte duas letras adjacentes
        chars = list(text)
        chars[idx], chars[idx+1] = chars[idx+1], chars[idx]
        return "".join(chars)
    else:
        # Remove uma letra
        return text[:idx] + text[idx+1:]

def generate_obitos(n_records: int) -> pd.DataFrame:
    """Gera a base de óbitos que será indexada no Elasticsearch."""
    logger.info(f"Gerando {n_records} registros para Óbitos (Index)...")
    data = []
    for _ in range(n_records):
        data.append({
            "codigo_obito": fake.uuid4(),
            "nome_completo": fake.name(),
            "nome_mae": fake.name_female(),
            "data_nascimento": fake.date_of_birth(minimum_age=0, maximum_age=90).strftime("%Y-%m-%d"),
            "data_obito": fake.date_between(start_date='-5y', end_date='today').strftime("%Y-%m-%d"),
            "municipio_obito": fake.city(),
            "uf_obito": random.choice(UFS_VALIDAS),
            "causa_basica": random.choice(["Trauma", "Parada Cardíaca", "Hemorragia", "Asfixia"])
        })
    return pd.DataFrame(data)

def generate_acidentes(df_obitos: pd.DataFrame, n_total: int, overlap_ratio: float) -> pd.DataFrame:
    """Gera a base de acidentes com 70% de duplicatas (vítimas fatais) e 30% de sobreviventes (sem link)."""
    n_overlap = int(n_total * overlap_ratio)
    n_new = n_total - n_overlap
    logger.info(f"Gerando {n_total} registros para Acidentes (Source)...")
    logger.info(f" -> {n_overlap} baseados no Index (com ruído) e {n_new} novos Fakes (sobreviventes).")

    # 1. Seleciona a amostra que será "linkada" (Vítimas que foram a óbito)
    df_overlap = df_obitos.sample(n=n_overlap, random_state=42).copy()
    
    # Substitui os IDs (Sistemas diferentes)
    df_overlap["codigo_acidente"] = [fake.uuid4() for _ in range(n_overlap)]
    
    # Mapeia UF para a coluna esperada
    df_overlap["uf_acidente"] = df_overlap["uf_obito"]
    df_overlap["municipio_acidente"] = df_overlap["municipio_obito"]
    
    # Gera uma data de acidente logicamente anterior ou igual à data do óbito
    # Para simplificar no fake, vamos apenas gerar uma data aleatória recente
    df_overlap["data_acidente"] = [fake.date_between(start_date='-6y', end_date='today').strftime("%Y-%m-%d") for _ in range(n_overlap)]
    df_overlap["gravidade"] = "Fatal"

    # Aplica o ruído nas colunas sensíveis de identificação
    df_overlap["nome_completo"] = df_overlap["nome_completo"].apply(introduce_noise)
    df_overlap["nome_mae"] = df_overlap["nome_mae"].apply(introduce_noise)
    
    # Remove colunas que pertencem exclusivamente à tabela de óbitos
    df_overlap = df_overlap[["codigo_acidente", "nome_completo", "nome_mae", "data_nascimento", "data_acidente", "uf_acidente", "municipio_acidente", "gravidade"]]

    # 2. Gera os 30% de dados totalmente novos (Acidentes sem óbito)
    new_data = []
    for _ in range(n_new):
        new_data.append({
            "codigo_acidente": fake.uuid4(),
            "nome_completo": fake.name(),
            "nome_mae": fake.name_female(),
            "data_nascimento": fake.date_of_birth(minimum_age=0, maximum_age=90).strftime("%Y-%m-%d"),
            "data_acidente": fake.date_between(start_date='-5y', end_date='today').strftime("%Y-%m-%d"),
            "uf_acidente": random.choice(UFS_VALIDAS),
            "municipio_acidente": fake.city(),
            "gravidade": random.choice(["Leve", "Moderado", "Grave"])
        })
    df_new = pd.DataFrame(new_data)

    # 3. Concatena e embaralha os registros
    df_final = pd.concat([df_overlap, df_new]).sample(frac=1, random_state=42).reset_index(drop=True)
    return df_final

if __name__ == "__main__":
    output_dir = get_input_path()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Geração
    df_obitos = generate_obitos(NUM_RECORDS_TARGET)
    df_acidentes = generate_acidentes(df_obitos, NUM_RECORDS_SOURCE, OVERLAP_RATIO)
    

    # Check
    logger.info(f"Óbitos: {df_obitos.shape[0]} registros gerados.")
    logger.info(f"Acidentes: {df_acidentes.shape[0]} registros gerados.")

    # Exportação em Parquet
    path_obitos = output_dir / "part-00000-obitos.parquet"
    path_acidentes = output_dir / "acidentes_example"
    
    logger.info(f"Salvando dados em {path_obitos} ")
    df_obitos.to_parquet(path_obitos, index=False)
    
    logger.info(f"Salvando dados em {path_acidentes} (particionado por uf_acidente)...")
    df_acidentes.to_parquet(path_acidentes, partition_cols=["uf_acidente"], index=False)
    
    logger.info("✅ Bancos de Acidentes e Óbitos gerados com sucesso!")