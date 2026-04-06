import httpx
import random
from faker import Faker

# Inicializa o Faker com localização para o Brasil
fake = Faker('pt_BR')

# Lista de produtos para combinar com palavras aleatórias e gerar nomes realistas
produtos_base = [
    "Monitor", "Teclado Mecanico", "Mouse Gamer", "Placa de Video",
    "Cadeira Gamer", "Headset", "Memoria RAM", "SSD NVMe", 
    "Fonte ATX", "Placa Mae", "Webcam", "Gabinete"
]

# URL da API local
URL_API = "http://127.0.0.1:8000/itens"
TOTAL_REGISTROS = 1000

def gerar_item():
    """Gera um dicionario com dados de um item (sem o campo ID)."""
    nome_produto = f"{random.choice(produtos_base)} {fake.word().capitalize()}"
    
    return {
        "nome": nome_produto,
        "preco": round(random.uniform(50.0, 4000.0), 2),
        "vendedor": fake.company(),
        "estoque": random.randint(0, 300)
    }

def executar_carga():
    print(f"Iniciando a carga de {TOTAL_REGISTROS} registros no banco de dados...")
    
    sucessos = 0
    falhas = 0
    
    with httpx.Client() as client:
        for i in range(TOTAL_REGISTROS):
            dados = gerar_item()
            try:
                resposta = client.post(URL_API, json=dados)
                
                if resposta.status_code == 201:
                    sucessos += 1
                    if sucessos % 100 == 0:
                        print(f"Progresso: {sucessos} registros inseridos com sucesso.")
                else:
                    falhas += 1
                    print(f"Falha na insercao do indice {i}: {resposta.text}")
                    
            except Exception as e:
                falhas += 1
                print(f"Erro de conexao no indice {i}: {e}")

    print("\nProcesso de carga inicial concluido.")
    print(f"Total de insercoes bem-sucedidas: {sucessos}")
    print(f"Total de falhas: {falhas}")

if __name__ == "__main__":
    executar_carga()