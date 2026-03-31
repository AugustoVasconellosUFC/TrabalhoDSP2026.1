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