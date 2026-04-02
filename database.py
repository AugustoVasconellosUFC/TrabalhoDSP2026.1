import os
import pyarrow as pa
from deltalake import write_deltalake

class DeltaDB:
    def __init__(self, table_path: str = "dados/itens_delta"):
        self.table_path = table_path
        # Garante que a pasta base exista
        os.makedirs("dados", exist_ok=True)

    def insert(self, item_dict: dict) -> None:
        """Insere um novo registro diretamente no arquivo (anexo/append)."""
        # Transforma o dicionário Pydantic num formato de colunas
        dados_colunares = {key: [value] for key, value in item_dict.items()}
        
        # Converte para tabela PyArrow
        tabela_arrow = pa.table(dados_colunares)

        # Escreve no Delta Lake anexando o novo dado sem reescrever tudo
        write_deltalake(self.table_path, tabela_arrow, mode="append")