import os
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import write_deltalake, DeltaTable
import csv
import io
import zipfile

class DeltaDB:
    def __init__(self, table_path: str = "dados/itens_delta"):
        self.table_path = table_path
        # Garante que a pasta base exista
        os.makedirs("dados", exist_ok=True)

    def insert(self, item_dict: dict) -> None:
        # Caminho para o ficheiro de controlo de IDs
        seq_file = "dados/sequence.seq"
        current_id = 1

        # Lê o ID atual (se o ficheiro existir)
        if os.path.exists(seq_file):
            with open(seq_file, "r") as f:
                conteudo = f.read().strip()
                if conteudo.isdigit():
                    current_id = int(conteudo) + 1

        # Atualiza o ficheiro com o novo ID
        with open(seq_file, "w") as f:
            f.write(str(current_id))

        # Injeta o ID gerado automaticamente no dicionário (chave 'id')
        item_dict["id"] = current_id

        # Grava no Delta Lake
        dados_colunares = {key: [value] for key, value in item_dict.items()}
        tabela_arrow = pa.table(dados_colunares)
        
        write_deltalake(self.table_path, tabela_arrow, mode="append")

    def list_paginated(self, page: int, page_size: int) -> list[dict]:
        """F2: Recupera uma página específica lendo apenas os lotes (batches) necessários sob demanda."""
        if not os.path.exists(self.table_path):
            return []

        dt = DeltaTable(self.table_path)
        dataset = dt.to_pyarrow_dataset()

        inicio = (page - 1) * page_size
        registros_retorno = []
        linhas_passadas = 0

        # Iteramos pelos lotes sem carregar o banco inteiro na memória RAM
        for batch in dataset.to_batches():
            # Condição de parada: se a página já está cheia, interrompemos a leitura do disco
            if len(registros_retorno) >= page_size:
                break

            # Se o lote inteiro está antes do nosso ponto de início (offset), apenas contabilizamos e pulamos
            if linhas_passadas + batch.num_rows <= inicio:
                linhas_passadas += batch.num_rows
                continue

            # Se chegamos aqui, o lote atual contém os dados que precisamos
            # Descobrimos o índice de partida dentro deste lote específico
            start_in_batch = max(0, inicio - linhas_passadas)
            
            # Calculamos quantos registros ainda faltam para completar o tamanho da página
            faltam = page_size - len(registros_retorno)
            
            # Fatiamos (slice) o lote extraindo apenas os registros úteis
            batch_slice = batch.slice(offset=start_in_batch, length=faltam)
            registros_retorno.extend(batch_slice.to_pylist())
            
            linhas_passadas += batch.num_rows

        return registros_retorno

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

        def vacuum(self):
            """Remove arquivos de dados antigos e versões obsoletas do log."""
            from deltalake import DeltaTable
            dt = DeltaTable(self.path)
            # Remove arquivos com mais de 168 horas (7 dias) por padrão
            dt.vacuum(retention_hours=168, enforce_retention_duration=False)