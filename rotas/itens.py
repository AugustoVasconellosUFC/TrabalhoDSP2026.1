from fastapi import APIRouter, Query, HTTPException
from http import HTTPStatus
from schemas import ItemCreate, ItemUpdate
from database import DeltaDB

router = APIRouter(prefix="/itens", tags=["Itens"])
db = DeltaDB()

@router.post("/", status_code=HTTPStatus.CREATED)
def criar_item(item: ItemCreate):
    item_dict = item.model_dump()
    db.insert(item_dict)
    return {"msg": "Item inserido com sucesso!", "dados": item_dict}

@router.get("/", status_code=HTTPStatus.OK)
def listar_itens(
    page: int = Query(1, ge=1, description="Numero da pagina"),
    page_size: int = Query(10, ge=1, le=100, description="Itens por pagina")
):
    registros = db.list_paginated(page, page_size)
    return {
        "pagina_atual": page,
        "tamanho_pagina": page_size,
        "total_nesta_pagina": len(registros),
        "dados": registros
    }

@router.get("/contagem", status_code=HTTPStatus.OK)
def obter_total_itens():
    total = db.count()
    return {"total_itens": total}

@router.get("/{item_id}", status_code=HTTPStatus.OK)
def buscar_item(item_id: int):
    item = db.get_by_id(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item nao encontrado")
    return item

@router.put("/{item_id}", status_code=HTTPStatus.OK)
def atualizar_item(item_id: int, item_atualizado: ItemUpdate):
    sucesso = db.update(item_id, item_atualizado.model_dump())
    if not sucesso:
        raise HTTPException(status_code=404, detail="Item nao encontrado")
    return {"msg": f"Item {item_id} atualizado!"}

@router.delete("/{item_id}", status_code=HTTPStatus.OK)
def deletar_item(item_id: int):
    sucesso = db.delete(item_id)
    if not sucesso:
        raise HTTPException(status_code=404, detail="Item nao encontrado")
    return {"msg": f"Item {item_id} deletado!"}