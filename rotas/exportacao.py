from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from stream_zip import ZIP_32, stream_zip
from stat import S_IFREG
from datetime import datetime
from database import DeltaDB

router = APIRouter(prefix="/exportar", tags=["Exportação"])
db = DeltaDB()

@router.get("/csv")
def exportar_csv():
    return StreamingResponse(
        db.generate_csv_stream(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=itens_exportados.csv"}
    )

@router.get("/csv-zip")
def exportar_csv_zip():
    def arquivos_para_zip():
        csv_bytes_stream = (linha.encode('utf-8') for linha in db.generate_csv_stream())
        yield (
            "data.csv",
            datetime.now(),
            S_IFREG | 0o600,
            ZIP_32,
            csv_bytes_stream 
        )

    csv_comprimido = stream_zip(arquivos_para_zip())

    return StreamingResponse(
        csv_comprimido,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=itens_exportados.zip"}
    )