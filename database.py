import os
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

class DeltaDB:
    def __init__(self, table_path: str = "dados/itens_delta"):
        self.table_path = table_path
        # Garante que a pasta base exista
        os.makedirs("dados", exist_ok=True)

    def insert(self, item_dict: dict) -> None:
        # Tudo abaixo precisa estar alinhado para o Python entender que pertence ao insert
        dados_colunares = {key: [value] for key, value in item_dict.items()}
        tabela_arrow = pa.table(dados_colunares)
        # O segredo é este mode="append"
        write_deltalake(self.table_path, tabela_arrow, mode="append")
        
    def list_paginated(self, page: int, page_size: int) -> list[dict]:
        """F2: Recupera uma página específica consolidando todos os arquivos parquet."""
        if not os.path.exists(self.table_path):
            return []

        # 1. Abre a tabela apontando para a pasta raiz
        dt = DeltaTable(self.table_path)
        
        # 2. Convertemos para um dataset do PyArrow que lê o LOG e descobre todos os arquivos
        dataset = dt.to_pyarrow_dataset()

        # 3. Em vez de percorrer batches (que podem vir fragmentados por arquivo),
        # carregamos a união de todos os dados em uma tabela temporária.
        tabela_unificada = dataset.to_table()
        
        # 4. Transformamos a tabela completa em uma lista de dicionários Python
        todos_os_registros = tabela_unificada.to_pylist()

        # 5. Aplicamos a lógica de paginação manualmente sobre a lista completa
        # Ex: Se page=1 e page_size=10, pega do índice 0 ao 10.
        inicio = (page - 1) * page_size
        fim = inicio + page_size
        
        return todos_os_registros[inicio:fim]