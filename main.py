from http import HTTPStatus
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

# WIP: importar deltalake e outras bibliotecas necessárias para o banco de dados
from database import DeltaDB

class Item(BaseModel):
    id: int = Field(..., description="ID do item", example=1)
    nome: str = Field(..., min_length=2, description="Nome do produto", example="Teclado Mecânico")
    preco: float = Field(..., gt=0, description="Preço unitário", example=250.50)
    vendedor: str = Field(..., description="Nome da loja", example="Loja de Informática")   # Em futuras implementações pode ser mudado para int
    estoque: int = Field(..., ge=0, description="Quantidade em estoque", example=15)

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
# F4	Contagem: retorna o número atual de registros da entidade.
# F5	Exportação CSV via streaming
# F6	Exportação CSV compactada via streaming
# F7	Hash de dado