import os
import yaml
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, lit   

from cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper import bootstrap_sequential_linkage

# --- FIXTURES DE AMBIENTE REAL COM SPARK LOCAL ---

@pytest.fixture(scope="module")
def spark_session():
    """Subirá uma sessão Spark local real para processar os DataFrames do teste de integração."""
    spark = SparkSession.builder \
        .appName("CidacsRL_BootstrapperUseCaseIntegration") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def setup_integration_env(tmp_path):
    """Gera o cenário físico de arquivos YAML e diretórios de dados Parquet para o teste."""
    configs_dir = tmp_path / "configs"
    data_input_dir = tmp_path / "data" / "input"
    data_output_dir = tmp_path / "data" / "output"
    
    os.makedirs(configs_dir, exist_ok=True)
    os.makedirs(data_input_dir, exist_ok=True)
    os.makedirs(data_output_dir, exist_ok=True)

    # 1. Escreve o arquivo com as regras de negócio de matching (linkage_specification.yml)
    linkage_business_content = {
        "source_table": "dataset_input_linkage",
        "id_source_table": "id_cidadao",
        "target_es_index": "indice_target_es",
        "id_target_table": "id_es",
        "blocking_phases": [
            {
                "phase_name": "fase_teste_integrado",
                "enabled": True,
                "candidate_limit": 10,
                "strong_match_score_threshold": 0.0,
                "rules": [
                    {
                        "source_column": "nome", 
                        "target_column": "nome",
                        "similarity": "exact",
                        "weight": 1.0,
                        "es_clause_type": "must",
                        "query_type": "match"
                    }
                ]
            }
        ]
    }
    linkage_specification_path = configs_dir / "linkage_specification.yml"
    with open(linkage_specification_path, "w") as f:
        yaml.dump(linkage_business_content, f)

    
    # 2. Cria o arquivo físico dummy do Elasticsearch exigido pelo bootstrapper
    es_dummy_content = {
        "es_connection_url": "http://localhost:9200",
        "username": "elastic",
        "password": "changeme"
    }
    es_config_path = configs_dir / "es_dummy.yml"
    with open(es_config_path, "w") as f:
        yaml.dump(es_dummy_content, f)

    # 3. Escreve o arquivo de ambiente e infraestrutura (sequential_linkage_workflow.yml)
    sequential_env_content = {
        "linkage_specification_path": str(linkage_specification_path),
        "es_config_path": str(es_config_path),
        "spark_config_path": str(configs_dir / "spark_dummy.yml"),
        "source_data_path": str(data_input_dir),
        "output_data_path": str(data_output_dir),
        "source_data_format": "parquet",
        "output_data_format": "parquet"
    }
    sequential_config_path = configs_dir / "sequential_linkage_workflow.yml"
    with open(sequential_config_path, "w") as f:
        yaml.dump(sequential_env_content, f)

    return {
        "config_path": str(sequential_config_path),
        "input_dir": str(data_input_dir),
        "output_dir": str(data_output_dir),
        "source_table": "dataset_input_linkage"
    }


# --- CASO DE TESTE DE INTEGRAÇÃO ---

class TestBootstrapperUseCaseIntegration:

    @patch("cidacsrl_rlp.cidacsrl.infra.bootstrappers.linkage_bootstrapper.get_es_client")
    @patch("cidacsrl_rlp.cidacsrl.infra.adapters.outbound.elasticsearch.spark_es_search_adapter.SparkESSearchAdapter.get_candidates")
    def test_complete_pipeline_execution_from_bootstrapper_to_use_case_output(
        self,
        mock_get_candidates_method,
        mock_get_es_client,
        spark_session,
        setup_integration_env
    ):
        """
        Teste de Integração: Garante que o bootstrapper inicializa a infraestrutura via YAML,
        injeta no Use Case, o Use Case executa as fases de blocagem e salva o resultado final no disco.
        """
        env = setup_integration_env

        # Simula que a conectividade preventiva do Elasticsearch está operando normalmente
        mock_get_es_client.return_value = MagicMock()

        # 1. PREPARAÇÃO DOS DADOS REAIS DE ENTRADA (PARQUET)
        input_df = spark_session.createDataFrame([
            ("1", "Maria Silva", "BA"),
            ("2", "João Santos", "SP"),
            ("3", "Carlos Souza", "RJ")
        ], ["id_cidadao", "nome", "uf"])
        
        table_physical_input_path = os.path.join(env["input_dir"], env["source_table"])
        input_df.write.mode("overwrite").format("parquet").save(table_physical_input_path)

        # 2. MOCK DOS CANDIDATOS DO ELASTICSEARCH (Porta de Saída de Busca)
        from pyspark.sql.functions import struct, lit

        # Massa de dados plana que representará a junção lógica
        base_candidates_df = spark_session.createDataFrame([
            ("1", "Maria Silva", "100", "Maria Silva", 0.98),
            ("2", "João Santos", "200", "Joao Santos", 0.95)
        ], ["id_cidadao", "nome_source", "id_es", "nome_target", "score_busca"])

        # Dados envelopados no esquema StructType complexo exigido pelo SparkScoringAdapter
        candidate_pairs_df = base_candidates_df.select(
            # Identificadores de controle do par
            base_candidates_df["id_cidadao"],
            base_candidates_df["id_es"],
            # StructType contendo o registro completo da origem
            struct(
                base_candidates_df["id_cidadao"],
                base_candidates_df["nome_source"].alias("nome")
            ).alias("source_record"),
            # StructType contendo o registro correspondente encontrado no ES
            struct(
                base_candidates_df["id_es"],
                base_candidates_df["nome_target"].alias("nome")
            ).alias("candidate_record"),
            # O score inicial gerado pelo Lucene/Elasticsearch
            base_candidates_df["score_busca"].alias("score")
        )
        
        # O método interceptado com o contrato exato de produção
        mock_get_candidates_method.return_value = candidate_pairs_df

        # 3. EXECUÇÃO DO GATILHO COMPLETO DO PIPELINE VIA BOOTSTRAPPER
        bootstrap_sequential_linkage(
            config_path=env["config_path"],
            spark_session=spark_session
        )

        # 4. VALIDAÇÃO DO RESULTADO INTEGRADO NO STORAGE DE SAÍDA (OUTPUT)
        expected_output_table = "phase_1_fase_teste_integrado"
        physical_output_path = os.path.join(env["output_dir"], expected_output_table)

        # IMPRESSÃO DE DIAGNÓSTICO: Se falhar, veremos no terminal o que o Spark realmente gravou na pasta de saída
        if not os.path.exists(physical_output_path):
            print(f"\n[DIAGNÓSTICO DIRETÓRIO DE OUTPUT]: {env['output_dir']}")
            if os.path.exists(env["output_dir"]):
                print(f"Arquivos realmente criados na raiz de output: {os.listdir(env['output_dir'])}")
            else:
                print("A pasta raiz de output sequer foi criada pelo adaptador.")

        # Verifica se o diretório de saída foi gerado em conformidade com o output_data_path do YAML
        assert os.path.exists(physical_output_path), f"O Use Case não gerou a pasta física esperada em: {physical_output_path}"
        
        # Lê e valida a integridade lógica da massa processada sem vazamento de argumentos técnicos
        output_df = spark_session.read.format("parquet").load(physical_output_path)
        
        assert output_df.count() == 2
        
        columns = output_df.columns
        assert "source_id_cidadao" in columns
        assert "candidate_id_es" in columns
        assert "match_score" in columns

        sample_match = output_df.filter(output_df.source_id_cidadao == "1").first()
        assert sample_match["candidate_id_es"] == "100"
        assert sample_match["match_score"] == 1.0