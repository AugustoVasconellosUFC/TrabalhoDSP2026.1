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
    # Transformar dados em str do stream de CSV para dados em bytes
    def csv_bytes_stream():
        for linha in db.generate_csv_stream():
            yield linha.encode('utf-8') 

    # Estabelecer parâmetros para função stream_zip
    def arquivos_para_zip():
        yield (
            "data.csv",                 # Nome do arquivo CSV dentro da pasta zip
            datetime.now(),             # Data de modificação do arquivo zipado
            S_IFREG | 0o600,            # Modo de compressão zip
            ZIP_32,                     # Tipo de compressão zip
            csv_bytes_stream()          # Stream de dados para o arquivo CSV zipado
        )

    csv_comprimido = stream_zip(arquivos_para_zip())

    return StreamingResponse(
        csv_comprimido,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=itens_exportados.zip"}
    )