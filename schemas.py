from pydantic import BaseModel, Field

class ItemCreate(BaseModel):
    nome: str = Field(..., min_length=2, description="Nome do produto")
    preco: float = Field(..., gt=0, description="Preço unitário")
    vendedor: str = Field(..., description="Nome da loja ou vendedor")
    estoque: int = Field(..., ge=0, description="Quantidade disponível em estoque")

class ItemResponse(ItemCreate):
    id: int = Field(..., description="ID único do item")

class ItemUpdate(BaseModel):
    """Modelo para atualização: todos os campos são opcionais."""
    nome: str | None = Field(None, min_length=2)
    preco: float | None = Field(None, gt=0)
    vendedor: str | None = Field(None)
    estoque: int | None = Field(None, ge=0)