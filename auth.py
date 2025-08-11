import os
import json
import hashlib

USERS_FILE = "usuarios.json"

def carregar_usuarios():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return {}

def salvar_usuarios(usuarios):
    with open(USERS_FILE, "w") as f:
        json.dump(usuarios, f)

def hash_senha(senha):
    return hashlib.sha256(senha.encode()).hexdigest()
