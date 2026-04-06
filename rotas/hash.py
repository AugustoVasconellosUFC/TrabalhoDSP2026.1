from fastapi import APIRouter
import hashlib

router = APIRouter(prefix="/hash", tags=["Hash"])

@router.get("/MD5/{value}")
def retornar_hash_MD5(value: str):
    hashed_value = hashlib.md5(value.encode('utf-8'))
    return hashed_value.hexdigest()

@router.get("/SHA-1/{value}")
def retornar_hash_SHA1(value: str):
    hashed_value = hashlib.sha1(value.encode('utf-8'))
    return hashed_value.hexdigest()

@router.get("/SHA-256/{value}")
def retornar_hash_SHA256(value: str):
    hashed_value = hashlib.sha256(value.encode('utf-8'))
    return hashed_value.hexdigest()