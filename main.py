from http import HTTPStatus
from fastapi import FastAPI
from pydantic import BaseModel
# WIP: importar deltalake e outras bibliotecas necessárias para o banco de dados

class Item(BaseModel):
    id: int
    nome: str
    preco: float
    vendedor: str   # Em futuras implementações pode ser mudado para int (id do vendedor)
    estoque: int

# WIP : banco de dados deltalake

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

# Funções
# F1	Inserção: recebe um JSON com os dados da entidade e insere no minibanco.
# F2	Listagem paginada: permite ao cliente definir número e tamanho da página via query string; retorna apenas a página solicitada. Sugestão: use .to_batches(batch_size=page_size) do deltalake.
# F3	CRUD completo seguindo as convenções REST (GET, POST, PUT/PATCH, DELETE). As operações devem agir diretamente no arquivo, sem reescrevê-lo integralmente.
# F4	Contagem: retorna o número atual de registros da entidade.
# F5	Exportação CSV via streaming: transmite todos os registros em formato CSV sem carregar o arquivo inteiro na memória.
# F6	Exportação CSV compactada via streaming: mesmo que F5, mas entregando o arquivo CSV compactado em .zip.
# F7	Hash de dado: recebe um valor e o nome do algoritmo (MD5, SHA-1 ou SHA-256) e retorna o hash correspondente.