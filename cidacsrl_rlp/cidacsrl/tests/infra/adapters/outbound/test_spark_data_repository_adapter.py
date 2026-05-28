import os
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from cidacsrl_rlp.cidacsrl.infra.adapters.outbound.spark_data_repository_adapter import SparkDataRepositoryAdapter

# --- FIXTURES DO PYTEST ---

@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark local ultraleve isolada para os testes unitários."""
    spark = SparkSession.builder \
        .appName("CidacsRL_UnitTests") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .get_mock_or_real() if hasattr(SparkSession.builder, "get_mock_or_real") else SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_env_config(tmp_path):
    """Gera caminhos temporários reais e seguros no disco usando a fixture tmp_path do pytest."""
    env_config = MagicMock()
    env_config.linkage_specification_path = str(tmp_path / "linkage_specification.yml")
    env_config.es_config_path = str(tmp_path / "elasticsearch_config.yml")
    env_config.source_data_path = str(tmp_path / "source_data")
    env_config.output_data_path = str(tmp_path / "output_data")
    env_config.source_data_format = "parquet"
    env_config.output_data_format = "parquet"
    env_config.partitioning = None
    env_config.sample_fraction = None
    
    # Cria os diretórios físicos para evitar erros de FileSystem iniciais
    os.makedirs(env_config.source_data_path, exist_ok=True)
    os.makedirs(env_config.output_data_path, exist_ok=True)
    return env_config

@pytest.fixture
def adapter(spark_session, mock_env_config):
    """Instancia o adaptador com as dependências mockadas e a sessão ativa do Spark."""
    return SparkDataRepositoryAdapter(spark_session=spark_session, env_config=mock_env_config)


# --- CASOS DE TESTE ---

class TestSparkDataRepositoryAdapter:

    # ─── 1. TESTES DE REQUISITO: READ_DATA (INGESTÃO) ───

    def test_read_data_success(self, spark_session, mock_env_config, adapter):
        """Garante que o adaptador consegue ler um arquivo Parquet de forma íntegra."""
        table_name = "test_table"
        table_path = os.path.join(mock_env_config.source_data_path, table_name)
        
        # Cria uma massa de dados de teste real e salva no caminho temporário
        original_df = spark_session.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        original_df.write.mode("overwrite").format("parquet").save(table_path)

        # Executa a leitura através do adaptador
        result_df = adapter.read_source_data(table_name=table_name)

        # Asserts de validação estrutural e de contagem
        assert result_df is not None
        assert result_df.count() == 2
        assert "name" in result_df.columns

    def test_read_data_file_not_found_raises_exception(self, adapter):
        """Valida se o Spark lança uma exceção esperada ao tentar ler uma tabela inexistente."""
        with pytest.raises(Exception):
            adapter.read_source_data(table_name="tabela_fantasma")


    # ─── 2. TESTES DE REQUISITO: WRITE_DATA (PERSISTÊNCIA) ───

    def test_write_data_success(self, spark_session, mock_env_config, adapter):
        """Garante que os dados passados pelo Use Case são gravados corretamente no diretório de output."""
        output_folder = "output_matches_fase_1"
        expected_output_path = os.path.join(mock_env_config.output_data_path, output_folder)
        
        df_to_save = spark_session.createDataFrame([("1092", 0.95)], ["codigo_internacao", "score"])

        # Executa a escrita
        adapter.write_data(data=df_to_save, output_folder=output_folder)

        # Verifica se os arquivos físicos foram criados pelo Spark no local esperado
        assert os.path.exists(expected_output_path)
        
        # Valida se o dado gravado pode ser relido e mantém a consistência
        check_df = spark_session.read.format("parquet").load(expected_output_path)
        assert check_df.count() == 1
        assert check_df.first()["score"] == 0.95


    # ─── 3. TESTES DE REQUISITO: EXCLUDE_RECORDS (TRANSFORMAÇÃO) ───

    def test_exclude_records_left_anti_join(self, spark_session, adapter):
        """Valida se o Left-Anti Join distribuído remove exatamente os registros passados como exclusão."""
        # Base Fonte (Registros Totais que entraram no pipeline)
        df_primary = spark_session.createDataFrame([
            ("A", "Salvador"),
            ("B", "Setúbal"),
            ("C", "Lisboa")
        ], ["id", "cidade"])

        # Lista de Exclusão (Registros que já foram encontrados/linkados na Fase 1)
        df_exclude = spark_session.createDataFrame([
            ("A", 0.99), # Já linkado
            ("C", 0.88)  # Já linkado
        ], ["id", "score"])

        # Aplica a transformação
        df_result = adapter.exclude_records(
            primary_dataset=df_primary,
            records_to_exclude=df_exclude,
            join_key="id"
        )

        # O resultado esperado deve conter APENAS o registro "B" (que não foi linkado na Fase 1)
        assert df_result.count() == 1
        assert df_result.first()["id"] == "B"
        assert df_result.first()["cidade"] == "Setúbal"


    # ─── 4. TESTES DE REQUISITO: CHECK_HEALTH (SANITY CHECK) ───

    def test_check_health_all_healthy(self, spark_session, mock_env_config, adapter):
        """Caminho feliz: Verifica se o método retorna uma lista vazia quando o input existe e o output aceita escrita."""
        source_table = "healthy_input"
        target_index = "healthy_output"
        
        # Cria preventivamente o arquivo Parquet de input exigido pela checagem
        input_path = os.path.join(mock_env_config.source_data_path, source_table)
        dummy_df = spark_session.createDataFrame([("test",)], ["col"])
        dummy_df.write.mode("overwrite").format("parquet").save(input_path)

        # Executa o check
        errors = adapter.check_health(source_table=source_table, target_index=target_index)

        # Sem erros mapeados
        assert isinstance(errors, list)
        assert len(errors) == 0

    def test_check_health_input_missing_returns_error(self, adapter):
        """Caminho de falha: Verifica se o erro de leitura é capturado de forma limpa na lista se o input sumir."""
        errors = adapter.check_health(source_table="tabela_inexistente", target_index="any_output")

        assert len(errors) == 1
        assert "Falha ao acessar o caminho" in errors[0]

    def test_check_health_output_unwritable_returns_error(self, spark_session, mock_env_config, adapter):
        """Caminho de falha: Verifica se o erro de permissão ou caminho inválido na escrita é capturado na lista."""
        source_table = "valid_input"
        
        # Cria o input para passar direto da primeira etapa do check
        input_path = os.path.join(mock_env_config.source_data_path, source_table)
        dummy_df = spark_session.createDataFrame([("test",)], ["col"])
        dummy_df.write.mode("overwrite").format("parquet").save(input_path)

        # Modifica o caminho de output para um local protegido ou impossível (ex: string vazia ou nula em ambiente controlado)
        # Para forçar um erro de I/O de escrita legítimo do Spark:
        adapter.env_config.output_data_path = "/root/diretorio_proibido_do_sistema"

        errors = adapter.check_health(source_table=source_table, target_index="target_failed")

        assert len(errors) == 1
        assert "Falha ao acessar o caminho" in errors[0]