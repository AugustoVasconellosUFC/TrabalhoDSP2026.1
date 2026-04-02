from pydantic import BaseModel, Field

class Item(BaseModel):
    id: int = Field(..., description="ID único do item", example=101)
    nome: str = Field(..., min_length=2, description="Nome do produto", example="Monitor Ultrawide 34 LG")
    preco: float = Field(..., gt=0, description="Preço unitário", example=2599.90)
    vendedor: str = Field(..., description="Nome da loja ou vendedor", example="Kabum Tecnologia")
    estoque: int = Field(..., ge=0, description="Quantidade disponível em estoque", example=15)