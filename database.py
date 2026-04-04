import os
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import write_deltalake, DeltaTable
import csv 
import io 

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

    # F3 - CRUD

    def get_by_id(self, item_id: int) -> dict | None:
        """Busca um registro específico aplicando filtro direto no disco."""
        if not os.path.exists(self.table_path): return None
        
        dt = DeltaTable(self.table_path)
        
        #Filtra no disco ANTES de virar tabela PyArrow
        tabela_filtrada = dt.to_pyarrow_dataset().to_table(filter=ds.field("id") == item_id)
        registros = tabela_filtrada.to_pylist()
        
        return registros[0] if registros else None

    def update(self, item_id: int, updates: dict) -> bool:
        """Atualiza um registro usando Predicate SQL."""
        if not os.path.exists(self.table_path): return False
        
        # Ignora campos None (para atualizar apenas o que foi enviado)
        updates_reais = {k: v for k, v in updates.items() if v is not None}
        if not updates_reais: return True
        
        # Formata para o Delta (Strings ganham aspas simples)
        sql_updates = {}
        for k, v in updates_reais.items():
            if isinstance(v, str):
                sql_updates[k] = f"'{v}'"
            else:
                sql_updates[k] = str(v)

        dt = DeltaTable(self.table_path)
        # O predicate diz: "Vá no disco e altere SOMENTE onde id = item_id"
        metricas = dt.update(predicate=f"id = {item_id}", updates=sql_updates)
        
        return metricas.get("num_updated_rows", 0) > 0

    def delete(self, item_id: int) -> bool:
        """Remove um registro usando Predicate SQL."""
        if not os.path.exists(self.table_path): return False
        
        dt = DeltaTable(self.table_path)
        # O predicate diz: "Vá no disco e apague SOMENTE onde id = item_id"
        metricas = dt.delete(predicate=f"id = {item_id}")
        
        return metricas.get("num_deleted_rows", 0) > 0
    
    def count(self):
        if not os.path.exists(self.table_path): return False
        
        dt = DeltaTable(self.table_path)
        
        return dt.to_pyarrow_dataset().count_rows()
    
    def generate_csv_stream(self):
        if not os.path.exists(self.table_path):
        # Em vez de apenas 'return', podemos emitir um log ou simplesmente
        # deixar o gerador terminar sem produzir chunks.
            return 

        dt = DeltaTable(self.table_path)
        
        # Se a tabela está vazia, o to_batches() pode não ter nada para iterar
        if dt.to_pyarrow_dataset().count_rows() == 0:
            return

        # O uso do yield abaixo transforma esta função oficialmente em um gerador
        batches = dt.to_pyarrow_dataset().to_batches()

        primeiro_batch = True
        for batch in batches:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=batch.schema.names)
            
            if primeiro_batch:
                writer.writeheader()
                primeiro_batch = False
            
            writer.writerows(batch.to_pylist())
            yield output.getvalue()
            output.close()