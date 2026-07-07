import logging
import os
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

from cidacsrl.config.logging import configure_logging

configure_logging()
logger = logging.getLogger("Script::DataGenerator")


# ─── Reprodutibilidade ─────────────────────────────────────────────────────────
# Semente única para os três geradores de aleatoriedade (stdlib random, Faker e
# amostragens do pandas). Sem isto os dados mudavam a cada execução, tornando as
# contagens de match do e2e não determinísticas.
SEED = int(os.environ.get("CIDACSRL_E2E_SEED", "42"))
random.seed(SEED)
Faker.seed(SEED)
fake = Faker("pt_BR")


# ─── Parâmetros de volume e mistura ────────────────────────────────────────────
NUM_RECORDS_TARGET = 10000   # Tamanho do índice do ES (Óbitos)
NUM_RECORDS_SOURCE = 5000    # Tamanho da tabela a ser linkada (Acidentes)
OVERLAP_RATIO = 0.70         # Fração de acidentes que são vítimas fatais (linkáveis)
FUZZY_RATIO = 0.55           # Fração do overlap que recebe ruído (só casa na fase fuzzy)
MULTI_LINK_CLONES = 300      # Acidentes-clone extras → vários acidentes p/ o mesmo óbito
                             # (many-to-one: gera clusters reais na deduplicação)

# Taxas de dados faltantes — exercitam o tratamento de nulos (penalty no scoring,
# skip de cláusula na query ES).
NULL_RATE_NOME_MAE = 0.08
NULL_RATE_NASCIMENTO = 0.06

# Municípios reais por UF — o município é sorteado dentro da UF do registro, de
# modo que a dupla (uf, município) seja geograficamente coerente (não um nome
# fictício de `fake.city()`). Alguns municípios representativos de cada estado.
MUNICIPIOS_POR_UF = {
    "AC": ["Rio Branco", "Cruzeiro do Sul", "Sena Madureira", "Tarauacá", "Feijó"],
    "AL": ["Maceió", "Arapiraca", "Palmeira dos Índios", "Rio Largo", "Penedo"],
    "AP": ["Macapá", "Santana", "Laranjal do Jari", "Oiapoque", "Mazagão"],
    "AM": ["Manaus", "Parintins", "Itacoatiara", "Manacapuru", "Coari"],
    "BA": ["Salvador", "Feira de Santana", "Vitória da Conquista", "Camaçari", "Itabuna"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte", "Maracanaú", "Sobral"],
    "DF": ["Brasília"],
    "ES": ["Vitória", "Vila Velha", "Serra", "Cariacica", "Cachoeiro de Itapemirim"],
    "GO": ["Goiânia", "Aparecida de Goiânia", "Anápolis", "Rio Verde", "Luziânia"],
    "MA": ["São Luís", "Imperatriz", "Timon", "Caxias", "Codó"],
    "MT": ["Cuiabá", "Várzea Grande", "Rondonópolis", "Sinop", "Tangará da Serra"],
    "MS": ["Campo Grande", "Dourados", "Três Lagoas", "Corumbá", "Ponta Porã"],
    "MG": ["Belo Horizonte", "Uberlândia", "Contagem", "Juiz de Fora", "Betim"],
    "PA": ["Belém", "Ananindeua", "Santarém", "Marabá", "Castanhal"],
    "PB": ["João Pessoa", "Campina Grande", "Santa Rita", "Patos", "Bayeux"],
    "PR": ["Curitiba", "Londrina", "Maringá", "Ponta Grossa", "Cascavel"],
    "PE": ["Recife", "Jaboatão dos Guararapes", "Olinda", "Caruaru", "Petrolina"],
    "PI": ["Teresina", "Parnaíba", "Picos", "Floriano", "Piripiri"],
    "RJ": ["Rio de Janeiro", "São Gonçalo", "Duque de Caxias", "Nova Iguaçu", "Niterói"],
    "RN": ["Natal", "Mossoró", "Parnamirim", "São Gonçalo do Amarante", "Ceará-Mirim"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Canoas", "Pelotas", "Santa Maria"],
    "RO": ["Porto Velho", "Ji-Paraná", "Ariquemes", "Vilhena", "Cacoal"],
    "RR": ["Boa Vista", "Rorainópolis", "Caracaraí", "Mucajaí", "Alto Alegre"],
    "SC": ["Florianópolis", "Joinville", "Blumenau", "São José", "Chapecó"],
    "SP": ["São Paulo", "Guarulhos", "Campinas", "São Bernardo do Campo", "Santo André"],
    "SE": ["Aracaju", "Nossa Senhora do Socorro", "Lagarto", "Itabaiana", "São Cristóvão"],
    "TO": ["Palmas", "Araguaína", "Gurupi", "Porto Nacional", "Paraíso do Tocantins"],
}
UFS_VALIDAS = list(MUNICIPIOS_POR_UF.keys())

# Causas externas (violentas) vs. não-violentas — usadas para derivar o booleano
# `obito_violento` de forma coerente com `causa_basica`.
CAUSAS_VIOLENTAS = ["Trauma", "Hemorragia", "Asfixia"]
CAUSAS_NAO_VIOLENTAS = ["Parada Cardíaca", "Septicemia"]
CAUSAS = CAUSAS_VIOLENTAS + CAUSAS_NAO_VIOLENTAS

# Local de ocorrência do óbito — campo puramente informativo, trazido ao resultado
# do linkage via `extra_target_fields` (não participa de nenhuma regra de match).
LOCAIS_OBITO = ["Hospital", "Domicílio", "Via Pública", "Outro Estab. Saúde", "Ignorado"]


def get_input_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "input"


def introduce_noise(text) -> str:
    """Aplica ruídos ortográficos para testar a similaridade Jaro-Winkler.

    Preserva `None`/strings muito curtas intactos (não há como trocar/remover
    caracteres com segurança), evitando o `randint(0, -1)` do gerador anterior.
    """
    # `not isinstance(text, str)` cobre None e o NaN (float) que o pandas usa para
    # células ausentes; strings com < 3 chars não têm par de letras seguro p/ trocar.
    if not isinstance(text, str) or len(text) < 3 or random.random() > 0.6:  # 60% mantém intacto
        return text

    idx = random.randint(0, len(text) - 2)
    action = random.choice(["swap", "delete"])

    if action == "swap":
        # Inverte duas letras adjacentes
        chars = list(text)
        chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
        return "".join(chars)
    # Remove uma letra
    return text[:idx] + text[idx + 1:]


def _maybe_null(value, null_rate: float):
    """Retorna None com probabilidade `null_rate`, senão o próprio valor."""
    return None if random.random() < null_rate else value


def generate_obitos(n_records: int) -> pd.DataFrame:
    """Gera a base de óbitos que será indexada no Elasticsearch.

    Datas são coerentes (nascimento antes do óbito) e há campos numérico
    (`idade_obito`) e booleano (`obito_violento`) para exercitar mapeamentos ES
    além de text/keyword/date.
    """
    logger.info(f"Gerando {n_records} registros para Óbitos (Index)...")
    data = []
    for _ in range(n_records):
        data_obito = fake.date_between(start_date="-5y", end_date="today")
        idade = random.randint(0, 95)
        data_nascimento = data_obito - timedelta(days=idade * 365 + random.randint(0, 364))

        causa = random.choice(CAUSAS)
        uf_obito = random.choice(UFS_VALIDAS)
        data.append({
            "codigo_obito": fake.uuid4(),
            "nome_completo": fake.name(),
            "nome_mae": _maybe_null(fake.name_female(), NULL_RATE_NOME_MAE),
            "data_nascimento": _maybe_null(data_nascimento.isoformat(), NULL_RATE_NASCIMENTO),
            "data_obito": data_obito.isoformat(),
            "municipio_obito": random.choice(MUNICIPIOS_POR_UF[uf_obito]),
            "uf_obito": uf_obito,
            "causa_basica": causa,
            "idade_obito": idade,
            "obito_violento": causa in CAUSAS_VIOLENTAS,
            "numero_declaracao": f"DO-{random.randint(10_000_000, 99_999_999)}",
            "local_obito": random.choice(LOCAIS_OBITO),
        })
    return pd.DataFrame(data)


def _acidente_from_obito(obito: pd.Series, apply_noise: bool) -> dict:
    """Constrói um registro de acidente derivado de um óbito (vítima fatal).

    Args:
        obito: Linha da base de óbitos que originou a vítima.
        apply_noise: Se True, injeta ruído ortográfico nos nomes (grupo fuzzy);
            se False, mantém os nomes idênticos (grupo exato).
    """
    # Normaliza células ausentes (pandas as representa como NaN float, não None).
    nome = obito["nome_completo"]
    mae_raw = obito["nome_mae"]
    mae = mae_raw if isinstance(mae_raw, str) else None
    dob_raw = obito["data_nascimento"]
    dob = dob_raw if isinstance(dob_raw, str) else None
    if apply_noise:
        nome = introduce_noise(nome)
        mae = introduce_noise(mae)

    data_obito = date.fromisoformat(obito["data_obito"])
    if dob:
        lower = date.fromisoformat(dob)
    else:
        lower = data_obito - timedelta(days=365 * 30)
    # 50% dos acidentes fatais ocorrem na mesma data do óbito (morte imediata);
    # o restante é anterior — mas sempre <= data do óbito.
    if random.random() < 0.5:
        data_acidente = data_obito
    else:
        data_acidente = fake.date_between(start_date=lower, end_date=data_obito)

    return {
        "codigo_acidente": fake.uuid4(),
        "nome_completo": nome,
        "nome_mae": mae,
        "data_nascimento": dob,
        "data_acidente": data_acidente.isoformat(),
        "uf_acidente": obito["uf_obito"],
        "municipio_acidente": obito["municipio_obito"],
        "gravidade": "Fatal",
    }


def _survivor_record() -> dict:
    """Gera um acidente sem correspondência em óbitos (sobrevivente)."""
    data_nascimento = fake.date_of_birth(minimum_age=0, maximum_age=90)
    uf_acidente = random.choice(UFS_VALIDAS)
    return {
        "codigo_acidente": fake.uuid4(),
        "nome_completo": fake.name(),
        "nome_mae": _maybe_null(fake.name_female(), NULL_RATE_NOME_MAE),
        "data_nascimento": _maybe_null(data_nascimento.isoformat(), NULL_RATE_NASCIMENTO),
        "data_acidente": fake.date_between(start_date="-5y", end_date="today").isoformat(),
        "uf_acidente": uf_acidente,
        "municipio_acidente": random.choice(MUNICIPIOS_POR_UF[uf_acidente]),
        "gravidade": random.choice(["Leve", "Moderado", "Grave"]),
    }


def generate_acidentes(df_obitos: pd.DataFrame, n_total: int, overlap_ratio: float) -> pd.DataFrame:
    """Gera a base de acidentes com três populações distintas.

    - Overlap exato: vítimas fatais com nomes íntegros → casam na fase exata.
    - Overlap fuzzy: vítimas fatais com ruído ortográfico → só na fase fuzzy.
    - Many-to-one: acidentes-clone apontando para óbitos já linkados do grupo
      exato → geram componentes conectados (clusters) na deduplicação.
    - Sobreviventes: acidentes sem óbito correspondente (sem link).
    """
    n_survivors = round(n_total * (1 - overlap_ratio))
    n_linked = n_total - n_survivors
    n_clones = min(MULTI_LINK_CLONES, max(0, n_linked // 4))
    n_unique = n_linked - n_clones
    n_fuzzy = round(n_unique * FUZZY_RATIO)

    logger.info(f"Gerando {n_total} registros para Acidentes (Source)...")
    logger.info(
        f" -> {n_unique} vítimas únicas ({n_fuzzy} com ruído / {n_unique - n_fuzzy} exatas), "
        f"{n_clones} clones many-to-one e {n_survivors} sobreviventes."
    )

    df_unique = df_obitos.sample(n=n_unique, random_state=SEED).reset_index(drop=True)

    linked_records = []
    exact_pool = []  # óbitos exatos (limpos) elegíveis a receber clones
    for i, obito in df_unique.iterrows():
        is_fuzzy = i < n_fuzzy
        linked_records.append(_acidente_from_obito(obito, apply_noise=is_fuzzy))
        if not is_fuzzy:
            exact_pool.append(obito)

    clone_records = []
    if exact_pool:
        for _ in range(n_clones):
            obito = exact_pool[random.randrange(len(exact_pool))]
            clone_records.append(_acidente_from_obito(obito, apply_noise=False))

    survivor_records = [_survivor_record() for _ in range(n_survivors)]

    df_final = (
        pd.DataFrame(linked_records + clone_records + survivor_records)
        .sample(frac=1, random_state=SEED)
        .reset_index(drop=True)
    )
    return df_final


if __name__ == "__main__":
    input_dir = get_input_path()

    path_obitos = input_dir / "obitos_example"
    path_acidentes = input_dir / "acidentes_example"

    path_obitos.mkdir(parents=True, exist_ok=True)

    # Geração
    df_obitos = generate_obitos(NUM_RECORDS_TARGET)
    df_acidentes = generate_acidentes(df_obitos, NUM_RECORDS_SOURCE, OVERLAP_RATIO)

    logger.info(f"Óbitos: {df_obitos.shape[0]} registros gerados.")
    logger.info(f"Acidentes: {df_acidentes.shape[0]} registros gerados.")

    # Exportação em Parquet
    logger.info(f"Salvando dados em {path_obitos} ...")
    df_obitos.to_parquet(path_obitos / "part-00000-obitos.parquet", index=False)

    logger.info(f"Salvando dados em {path_acidentes} (particionado por uf_acidente)...")
    df_acidentes.to_parquet(path_acidentes, partition_cols=["uf_acidente"], index=False)

    logger.info("✅ Bancos de Acidentes e Óbitos gerados com sucesso!")
