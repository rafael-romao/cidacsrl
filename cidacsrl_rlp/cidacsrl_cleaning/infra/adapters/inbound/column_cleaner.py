import logging
from typing import List, Optional, Callable, Union
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    regexp_replace,
    when,
    trim,
    upper,
    lower,
    initcap,
    translate,
    substring,
    lit,
    md5,
    to_json,
    struct,
)
from functools import partial

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class ColumnConfig:
    """Define as operações de limpeza para uma única coluna.

    Attributes:
        name (str): O nome original da coluna no DataFrame.
        cleaned_name (Optional[str]): O novo nome da coluna após a limpeza.
            Se não for fornecido, o nome original é usado.
        invalid_value (Optional[str]): Um valor específico a ser substituído por nulo.
        standardize_case (Optional[str]): Padroniza o texto para 'upper', 'lower' ou 'title'.
        replace_empty_with_null (bool): Se True, substitui strings vazias por nulo.
        cast_to (Optional[str]): Converte a coluna para um tipo de dado Spark (ex: 'integer').
        chars_to_remove (Optional[str]): Uma string de caracteres a serem removidos.
        normalize_chars (bool): Se True, remove acentos e caracteres especiais.
        truncate_length (Optional[int]): Trunca a coluna para um comprimento máximo.
    """
    name: str
    cleaned_name: Optional[str] = None
    invalid_value: Optional[str] = None
    standardize_case: Optional[str] = None  # upper, lower, title
    replace_empty_with_null: bool = False
    cast_to: Optional[str] = None
    chars_to_remove: Optional[str] = None
    normalize_chars: bool = False
    truncate_length: Optional[int] = None

    def __post_init__(self):
        if self.cleaned_name is None:
            self.cleaned_name = self.name


@dataclass
class ConcatenateColumnConfig:
    """Define a operação de concatenação de múltiplas colunas.

    Attributes:
        name (str): O nome da nova coluna que conterá o resultado da concatenação.
        columns (List[str]): Uma lista dos nomes das colunas a serem concatenadas.
        separator (str): O separador a ser usado entre os valores das colunas.
    """
    name: str
    columns: List[str]
    separator: str = " "

    def __post_init__(self):
        if not self.columns:
            raise ValueError("columns must be provided for ConcatenateColumnConfig.")
        if len(self.columns) < 2:
            raise ValueError(
                "At least two source columns must be provided for concatenation."
            )
        if not isinstance(self.columns, list):
            raise ValueError("columns must be a list of columns.")
        if not all(isinstance(col, str) for col in self.columns):
            raise ValueError("All columns must be strings.")
        if not isinstance(self.separator, str):
            raise ValueError("The separator must be a string.")
        if not self.name:
            raise ValueError("A name must be provided for the concatened column.")


class ColumnCleanerPipeline:
    """Constrói e executa um pipeline de limpeza de dados para um DataFrame PySpark.

    Esta classe recebe uma lista de configurações de coluna (`ColumnConfig` e
    `ConcatenateColumnConfig`) e gera uma sequência de etapas de transformação.
    Cada etapa é uma função que aplica uma operação de limpeza específica
    (ex: trim, padronização de caixa, concatenação) a uma ou mais colunas.

    O pipeline é executado sequencialmente através do método `apply`. As
    transformações são aplicadas diretamente nas colunas existentes, e colunas
    são renomeadas no final do processo se um `cleaned_name` diferente for
    fornecido.
    """

    def __init__(
        self, columns_config: List[Union[ColumnConfig, ConcatenateColumnConfig]]
    ):
        """Inicializa o ColumnCleanerPipeline.

        Args:
            columns_config (List[Union[ColumnConfig, ConcatenateColumnConfig]]):
                Uma lista de objetos `ColumnConfig` e `ConcatenateColumnConfig`
                que definem as etapas de limpeza e concatenação a serem executadas.
        """
        self.columns: List[ColumnConfig] = [
            col for col in columns_config if isinstance(col, ColumnConfig)
        ]
        self.concatenate_configs: List[ConcatenateColumnConfig] = [
            col for col in columns_config if isinstance(col, ConcatenateColumnConfig)
        ]
        self.steps: List[Callable[[DataFrame], DataFrame]] = self._generate_steps()

    def _generate_steps(self) -> List[Callable[[DataFrame], DataFrame]]:
        """Gera a lista de funções de limpeza com base nas configurações.

        Internamente, constrói a sequência de todas as transformações a serem
        aplicadas, começando com a adição de um ID único, seguido pelas
        limpezas de coluna individuais e, finalmente, pelas operações de
        concatenação.

        Returns:
            List[Callable[[DataFrame], DataFrame]]: Uma lista de funções, onde
                cada função recebe um DataFrame e retorna um DataFrame transformado.
        """
        steps: List[Callable[[DataFrame], DataFrame]] = []

        # Add unique ID based on the original columns
        first_step = partial(
            ColumnCleanerFunctions.add_unique_id,
            columns=[col.name for col in self.columns],
        )
        steps.append(first_step)

        for column in self.columns:
            next_steps = self._generate_column_steps(column)
            steps.extend(next_steps)

        if self.concatenate_configs:
            for config in self.concatenate_configs:
                last_steps = partial(
                    ColumnCleanerFunctions.concatenate_columns,
                    columns=config.columns,
                    output_column=config.name,
                    separator=config.separator,
                )
                steps.append(last_steps)

        return steps

    def _generate_column_steps(
        self, column: ColumnConfig
    ) -> List[Callable[[DataFrame], DataFrame]]:
        """Gera as etapas de limpeza para uma única coluna.

        Com base em um objeto `ColumnConfig`, esta função cria uma lista de
        funções de transformação específicas para essa coluna. As transformações
        são aplicadas diretamente na coluna original, e uma etapa de renomeação
        é adicionada ao final se `cleaned_name` for diferente de `name`.

        Args:
            column (ColumnConfig): O objeto `ColumnConfig` que define as etapas de limpeza.

        Returns:
            List[Callable[[DataFrame], DataFrame]]: Uma lista de funções de limpeza
                para a coluna especificada.
        """
        target_column = column.name

        steps: List[Callable[[DataFrame], DataFrame]] = [
            partial(ColumnCleanerFunctions.trim_column, column=target_column),
        ]

        if column.standardize_case:
            steps.append(
                partial(
                    ColumnCleanerFunctions.standardize_case,
                    column=target_column,
                    case_type=column.standardize_case,
                )
            )

        optional_steps = [
            (
                column.invalid_value,
                ColumnCleanerFunctions.replace_invalid_value,
                {"column": target_column, "invalid_value": column.invalid_value},
            ),
            (
                column.cast_to,
                ColumnCleanerFunctions.cast_column,
                {"column": target_column, "data_type": column.cast_to},
            ),
            (
                column.chars_to_remove,
                ColumnCleanerFunctions.remove_chars,
                {"column": target_column, "chars": column.chars_to_remove},
            ),
            (
                column.normalize_chars,
                ColumnCleanerFunctions.normalize_chars,
                {"column": target_column},
            ),
            (
                column.truncate_length,
                ColumnCleanerFunctions.truncate_length,
                {"column": target_column, "col_length": column.truncate_length},
            ),
        ]

        steps.extend(
            partial(func, **kwargs)
            for condition, func, kwargs in optional_steps
            if condition
        )

        steps.append(
            partial(
                ColumnCleanerFunctions.replace_empty_with_null,
                column=target_column,
            )
        )

        if column.cleaned_name != column.name:
             steps.append(
                 partial(
                     ColumnCleanerFunctions.rename_column,
                     old_column_name=column.name,
                     new_column_name=column.cleaned_name
                 )
             )

        return steps

    def apply(self, df: DataFrame) -> DataFrame:
        """Aplica a sequência de etapas de limpeza a um DataFrame.

        Executa cada função de transformação no pipeline em ordem, passando o
        DataFrame resultante de uma etapa como entrada para a próxima.

        Args:
            df (DataFrame): O DataFrame PySpark de entrada a ser limpo.

        Returns:
            DataFrame: O DataFrame limpo após a aplicação de todas as etapas.

        Raises:
            ValueError: Se ocorrer um erro de valor durante a aplicação de uma etapa.
            TypeError: Se ocorrer um erro de tipo durante a aplicação de uma etapa.
            KeyError: Se uma coluna não for encontrada durante a aplicação de uma etapa.
        """
        total_steps = len(self.steps)
        for i, step in enumerate(self.steps):
            func_name = getattr(getattr(step, 'func', None), '__name__', 'unknown_function')
            keywords = getattr(step, 'keywords', {})
            try:
                logger.info(f"Applying step {i+1}/{total_steps}: {func_name}")
                df = step(df)
            except (ValueError, TypeError, KeyError) as e:
                logger.error(
                    f"Error applying step {i+1}/{total_steps} ({func_name}) with arguments {keywords}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.error(
                    f"Unexpected error applying step {i+1}/{total_steps} ({func_name}) with arguments {keywords}: {e}",
                    exc_info=True,
                )
                raise
        return df

    def __repr__(self) -> str:
        """Retorna uma representação em string das etapas do pipeline.

        Returns:
            str: A representação do pipeline.
        """
        step_reprs = []
        for i, step in enumerate(self.steps):
            func_name = getattr(getattr(step, 'func', None), '__name__', 'unknown_function')
            keywords = getattr(step, 'keywords', {})
            args_repr = ", ".join(f"{k}={v!r}" for k, v in keywords.items())
            step_reprs.append(f"  Step {i+1}: {func_name}({args_repr})")
        return f"ColumnCleanerPipeline with {len(self.steps)} steps:\n" + "\n".join(
            step_reprs
        )


class ColumnCleanerFunctions:
    """Uma coleção de métodos estáticos para realizar operações de limpeza de dados.

    Cada método nesta classe implementa uma transformação atômica em uma coluna
    de um DataFrame PySpark (ex: trim, cast, remoção de caracteres) e retorna
    o DataFrame modificado.
    """

    @staticmethod
    def rename_column(df: DataFrame, old_column_name: str, new_column_name: str) -> DataFrame:
        """Renomeia uma coluna em um DataFrame.

        Args:
            df (DataFrame): O DataFrame de entrada.
            old_column_name (str): O nome atual da coluna.
            new_column_name (str): O novo nome para a coluna.

        Returns:
            DataFrame: O DataFrame com a coluna renomeada.
        """
        logger.info(f"Renaming column '{old_column_name}' to '{new_column_name}'")
        return df.withColumnRenamed(old_column_name, new_column_name)

    @staticmethod
    def trim_column(df: DataFrame, column: str) -> DataFrame:
        """Remove espaços em branco no início e no final de uma coluna string.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser processada.

        Returns:
            DataFrame: O DataFrame com a coluna processada.
        """
        logger.info(f"Trimming column: {column}")
        return df.withColumn(column, trim(col(column)))

    @staticmethod
    def replace_invalid_value(
        df: DataFrame, column: str, invalid_value: str
    ) -> DataFrame:
        """Substitui um valor específico considerado inválido por nulo.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser processada.
            invalid_value (str): O valor exato a ser substituído por nulo.

        Returns:
            DataFrame: O DataFrame com os valores inválidos substituídos.
        """
        logger.info(f"Replacing invalid value '{invalid_value}' in column: {column}")
        return df.withColumn(
            column,
            when(col(column) == invalid_value, lit(None).cast('string')).otherwise(
                col(column)
            ),
        )

    @staticmethod
    def replace_empty_with_null(df: DataFrame, column: str) -> DataFrame:
        """Substitui strings vazias ("") por nulo em uma coluna.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser processada.

        Returns:
            DataFrame: O DataFrame com as strings vazias substituídas.
        """
        logger.info(f"Replacing empty strings with null in column: {column}")
        return df.withColumn(
            column,
            when(trim(col(column)) == "", lit(None).cast(df.schema[column].dataType))
            .otherwise(col(column)),
        )

    @staticmethod
    def standardize_case(df: DataFrame, column: str, case_type: str) -> DataFrame:
        """Padroniza o texto de uma coluna para maiúsculas, minúsculas ou título.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser padronizada.
            case_type (str): O tipo de padronização ('upper', 'lower', 'title').

        Returns:
            DataFrame: O DataFrame com a coluna padronizada.

        Raises:
            ValueError: Se `case_type` for inválido.
        """
        logger.info(f"Standardizing case for column: {column} to {case_type}")
        case_functions = {"upper": upper, "lower": lower, "title": initcap}
        case_function = case_functions.get(case_type.lower())
        if not case_function:
            raise ValueError(
                f"Invalid case_type '{case_type}'. Choose from 'upper', 'lower', or 'title'."
            )
        return df.withColumn(column, case_function(col(column)))

    @staticmethod
    def remove_chars(df: DataFrame, column: str, chars: str) -> DataFrame:
        """Remove um conjunto de caracteres específicos de uma coluna.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser processada.
            chars (str): Uma string contendo os caracteres a serem removidos,
                formatada como uma expressão regular (ex: '[-_.]').

        Returns:
            DataFrame: O DataFrame com os caracteres removidos da coluna.
        """
        logger.info(f"Removing characters from column: {column}")
        return df.withColumn(column, regexp_replace(col(column), f"{chars}", ""))

    @staticmethod
    def normalize_chars(df: DataFrame, column: str) -> DataFrame:
        """Normaliza caracteres, removendo acentos e cedilha.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser normalizada.

        Returns:
            DataFrame: O DataFrame com os caracteres da coluna normalizados.
        """
        logger.info(f"Normalizing characters in column: {column}")
        from_chars = "ÁáàâäÃãÀàÂâÄäÉéèêëÊêËëÍíìîïÎîÏïÓóòôöõÕõÒòÔôÖöÚúùûüŨũÙùÛûÜüÇç"
        to_chars = "AaaaaAaAaAaAaEeeeeEeEeIiiiiIiIiOoooooOoOoOoOoUuuuuUuUuUuUuCc"
        return df.withColumn(column, translate(col(column), from_chars, to_chars))

    @staticmethod
    def truncate_length(df: DataFrame, column: str, col_length: int) -> DataFrame:
        """Trunca o conteúdo de uma coluna para um comprimento máximo.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser truncada.
            col_length (int): O comprimento máximo desejado para a coluna.

        Returns:
            DataFrame: O DataFrame com a coluna truncada.
        """
        if col_length <= 0:
             logger.warning(f"Truncate length for column {column} is non-positive ({col_length}). Skipping truncation.")
             return df
        logger.info(f"Truncating length of column: {column} to {col_length} characters")
        return df.withColumn(
            column,
            substring(col(column), 1, col_length)
        )

    @staticmethod
    def concatenate_columns(
        df: DataFrame, columns: List[str], output_column: str, separator: str = " "
    ) -> DataFrame:
        """Concatena múltiplas colunas em uma nova coluna.

        Args:
            df (DataFrame): O DataFrame de entrada.
            columns (List[str]): Lista com os nomes das colunas a serem concatenadas.
            output_column (str): O nome da nova coluna que conterá o resultado.
            separator (str): O separador a ser usado entre os valores.

        Returns:
            DataFrame: O DataFrame com a nova coluna concatenada.

        Raises:
            ValueError: Se alguma das colunas de entrada não existir no DataFrame.
        """
        logger.info(f"Concatenating columns: {columns} into {output_column}")
        for c in columns:
            if c not in df.columns:
                raise ValueError(f"Input column '{c}' for concatenation not found in DataFrame.")
        return df.withColumn(
            output_column, concat_ws(separator, *[col(c) for c in columns])
        )

    @staticmethod
    def add_unique_id(
        df: DataFrame, columns: List[str], id_column: str = "id_table"
    ) -> DataFrame:
        """Adiciona uma coluna de ID único com base no hash de outras colunas.

        O ID é gerado calculando o hash MD5 da representação JSON de uma
        estrutura contendo as colunas especificadas.

        Args:
            df (DataFrame): O DataFrame de entrada.
            columns (List[str]): Lista de colunas a serem usadas para gerar o ID.
            id_column (str): O nome da nova coluna de ID.

        Returns:
            DataFrame: O DataFrame com a nova coluna de ID.

        Raises:
            ValueError: Se alguma das colunas de entrada não existir no DataFrame.
        """
        logger.info(
            f"Adding unique ID column: {id_column} based on columns: md5(to_json(struct({columns})))"
        )
        for c in columns:
            if c not in df.columns:
                raise ValueError(f"Input column '{c}' for unique ID generation not found in DataFrame.")

        return df.withColumn(
            id_column, md5(to_json(struct(*[col(c) for c in columns])))
        )

    @staticmethod
    def cast_column(df: DataFrame, column: str, data_type: str) -> DataFrame:
        """Converte uma coluna para um tipo de dado específico.

        Se a conversão falhar, um erro é registrado e o DataFrame original é
        retornado sem alterações na coluna.

        Args:
            df (DataFrame): O DataFrame de entrada.
            column (str): O nome da coluna a ser convertida.
            data_type (str): O tipo de dado do Spark para o qual converter
                (ex: 'integer', 'date').

        Returns:
            DataFrame: O DataFrame com a coluna convertida ou o DataFrame
                original em caso de erro.
        """
        logger.info(f"Casting column: {column} to {data_type}")
        try:
            return df.withColumn(column, col(column).cast(data_type))
        except Exception as e:
            logger.error(
                f"Error casting column '{column}' to '{data_type}': {e}. Keeping original type.",
                exc_info=True,
            )
            return df
