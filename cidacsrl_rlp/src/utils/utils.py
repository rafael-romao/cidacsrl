import re


def sanitize_string(name: str) -> str:
    """
    Remove caracteres inválidos de uma string para que ela possa ser usada
    com segurança como parte de um nome de diretório ou arquivo.
    Caracteres inválidos são substituídos por underscores.

    Args:
        name (str): A string original a ser sanitizada.

    Returns:
        str: A string sanitizada, segura para uso em nomes de arquivo/diretório.
             Retorna "unnamed_component" se a string de entrada for vazia ou None.
             Retorna "sanitized_empty" se a string se tornar vazia após a sanitização.
    """
    if not name:
        return "unnamed_component" # Handles None or empty string input
    # Remove or replace characters invalid in Windows/Linux filenames
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace whitespace sequences with a single underscore
    name = re.sub(r'\s+', '_', name)
    # Strip leading/trailing underscores, dots, or whitespace (though whitespace should be gone)
    name = name.strip('._ ')
    return name if name else "sanitized_empty"