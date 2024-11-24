from fastapi import APIRouter
from Models import cassandraModel as cs
from Models import dgraphModel as dg
from Models import dbModel as mg


router = APIRouter()


@router.post("/add_user")
def add_user(user_id: int, name: str, email: str, password: str):
    pass