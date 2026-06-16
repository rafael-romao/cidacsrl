from dataclasses import dataclass
from typing import List, Optional



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