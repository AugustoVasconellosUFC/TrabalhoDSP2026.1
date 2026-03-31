from http import HTTPStatus
from fastapi import FastAPI
from pydantic import BaseModel

class Item(BaseModel):
    id: int
    nome: str
    preco: float
    vendedor: str   # Em futuras implementações pode ser mudado para int (id do vendedor)
    estoque: int


# WIP : banco de dados


app = FastAPI()

@app.get("/")
def home():   # Função de teste
    return{"msg":"Olá, mundo!"}


# WIP : API CRUD


# insert -- Insere um novo registro

# get -- Recupera um registro por ID

# list -- Retorna uma página de registros, conforme número e tamanho de página informados

# update -- Atualiza um registro existente

# delete -- Remove um registro

# count -- Retorna o total real de registros armazenados

# vacuum -- Compacta e limpa o arquivo de dados, descartando versões antigas, recuperando espaço em disco e mantendo o desempenho a longo prazo