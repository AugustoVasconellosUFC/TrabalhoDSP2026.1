from http import HTTPStatus
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field
from fastapi.responses import StreamingResponse
from stream_zip import stream_zip
import hashlib
# WIP: importar deltalake e outras bibliotecas necessárias para o banco de dados
from database import DeltaDB

class Item(BaseModel):
    id: int = Field(..., description="ID do item", example=1)
    nome: str = Field(..., min_length=2, description="Nome do produto", example="Teclado Mecânico")
    preco: float = Field(..., gt=0, description="Preço unitário", example=250.50)
    vendedor: str = Field(..., description="Nome da loja", example="Loja de Informática")   # Em futuras implementações pode ser mudado para int
    estoque: int = Field(..., ge=0, description="Quantidade em estoque", example=15)
    
class ItemUpdate(BaseModel):
    """Modelo para atualização: todos os campos são opcionais (None)."""
    nome: str | None = Field(None, min_length=2, description="Nome do produto")
    preco: float | None = Field(None, gt=0, description="Preço unitário")
    vendedor: str | None = Field(None, description="Nome da loja")
    estoque: int | None = Field(None, ge=0, description="Quantidade em estoque")

# WIP : banco de dados deltalake
db = DeltaDB()

# Métodos
# insert -- Insere um novo registro
# get -- Recupera um registro por ID
# list -- Retorna uma página de registros, conforme número e tamanho de página informados
# update -- Atualiza um registro existente
# delete -- Remove um registro
# count -- Retorna o total real de registros armazenados
# vacuum -- Compacta e limpa o arquivo de dados, descartando versões antigas, recuperando espaço em disco e mantendo o desempenho a longo prazo

app = FastAPI()

# Função de teste
@app.get("/")
def home():
    return{"msg":"Olá, mundo!"}

# WIP : API REST

# F1 - Inserção

@app.post("/itens", status_code=HTTPStatus.CREATED)
def criar_item(item: Item):
    """F1 - Recebe um JSON com os dados da entidade e insere no minibanco."""
    db.insert(item.model_dump())
    return {"msg": "Item inserido com sucesso!", "dados": item}

# F2 - Listagem Paginada

@app.get("/itens", status_code=HTTPStatus.OK)
def listar_itens(
    page: int = Query(1, ge=1, description="Número da página solicitada"),
    page_size: int = Query(10, ge=1, le=100, description="Quantidade de itens por página")
):
    """F2 - Retorna apenas a página solicitada controlada via query string."""
    registros = db.list_paginated(page, page_size)
    
    return {
        "pagina_atual": page,
        "tamanho_pagina": page_size,
        "total_nesta_pagina": len(registros),
        "dados": registros
    }

# F3	CRUD completo seguindo as convenções REST (GET, POST, PUT/PATCH, DELETE).

@app.get("/itens/contagem", status_code=HTTPStatus.OK)
def obter_total_itens():    
    total = db.count()
    return {"total_itens": total}

@app.get("/itens/{item_id}", status_code=HTTPStatus.OK)
def buscar_item(item_id: int):
    """F3 - Retorna um registro específico pelo ID."""
    item = db.get_by_id(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item não encontrado")
    return item

@app.put("/itens/{item_id}", status_code=HTTPStatus.OK)
def atualizar_item(item_id: int, item_atualizado: ItemUpdate):
    """F3 - Atualiza campos específicos de um registro existente."""
    sucesso = db.update(item_id, item_atualizado.model_dump())
    if not sucesso:
        raise HTTPException(status_code=404, detail="Item não encontrado ou ID inválido")
    return {"msg": f"Item {item_id} atualizado com sucesso!"}

@app.delete("/itens/{item_id}", status_code=HTTPStatus.OK)
def deletar_item(item_id: int):
    """F3 - Remove um registro do banco de dados."""
    sucesso = db.delete(item_id)
    if not sucesso:
        raise HTTPException(status_code=404, detail="Item não encontrado ou ID inválido")
    return {"msg": f"Item {item_id} deletado com sucesso!"}

# F4	Contagem: retorna o número atual de registros da entidade.

@app.get("/itens/contagem", status_code=HTTPStatus.OK)
def obter_total_itens():    
    total = db.count()
    return {"total_itens": total}

# F5	Exportação CSV via streaming

@app.get("/itens/exportar/csv")
def exportar_csv():
    return StreamingResponse(
        db.generate_csv_stream(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=itens_exportados.csv"}
    )

# F6	Exportação CSV compactada via zip

@app.get("/itens/exportar/csv-zip")
def exportar_csv_zip():
    return StreamingResponse(
        stream_zip(db.generate_csv_stream()),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=itens_exportados.zip"}
    )

# F7	Hash de dado

@app.get("/hash/MD5/{value}")
def retornar_hash_MD5(value: str):
    hashed_value = hashlib.md5(value.encode('utf-8'))
    return hashed_value.hexdigest()

@app.get("/hash/SHA-1/{value}")
def retornar_hash_SHA1(value: str):
    hashed_value = hashlib.sha1(value.encode('utf-8'))
    return hashed_value.hexdigest()

@app.get("/hash/SHA-256/{value}")
def retornar_hash_SHA256(value: str):
    hashed_value = hashlib.sha256(value.encode('utf-8'))
    return hashed_value.hexdigest()