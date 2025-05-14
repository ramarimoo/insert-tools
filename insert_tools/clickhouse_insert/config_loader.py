
import json
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel
import yaml
from dotenv import load_dotenv

class InsertConfigModel(BaseModel):
    host: str
    database: str
    target_table: str
    select_sql: str
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 8123
    allow_type_cast: bool = False

def load_config(path: str) -> InsertConfigModel:
    ext = Path(path).suffix.lower()

    if ext == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

    elif ext in (".yaml", ".yml"):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

    elif ext == ".env":
        load_dotenv(dotenv_path=path)
        data = {
            "host": os.getenv("HOST"),
            "database": os.getenv("DATABASE"),
            "target_table": os.getenv("TARGET_TABLE"),
            "select_sql": os.getenv("SELECT_SQL"),
            "user": os.getenv("USER"),
            "password": os.getenv("PASSWORD"),
            "port": int(os.getenv("PORT", 8123)),
            "allow_type_cast": os.getenv("ALLOW_TYPE_CAST", "false").lower() == "true",
        }
    else:
        raise ValueError(f"Unsupported config format: {ext}")

    return InsertConfigModel(**data)
