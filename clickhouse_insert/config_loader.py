
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
    data: Dict[str, Any] = {}

    try:
        if ext == ".json":
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

        elif ext in (".yaml", ".yml"):
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

        elif ext == ".env":
            load_dotenv(dotenv_path=path)

            env_map = {
                "HOST": "host",
                "DATABASE": "database",
                "TARGET_TABLE": "target_table",
                "SELECT_SQL": "select_sql",
                "USER": "user",
                "PASSWORD": "password",
                "PORT": "port",
                "ALLOW_TYPE_CAST": "allow_type_cast",
            }
            
            for env_key, model_key in env_map.items():
                value = os.getenv(env_key)
                if value is not None:
                    data[model_key] = value

        else:
            raise ValueError(f"Unsupported config format: {ext}. Must be .json, .yaml, .yml or .env")

    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {path}")
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise ValueError(f"Error parsing config file {path}: {e}")

    try:
        return InsertConfigModel(**data)
    except ValidationError as e:
        raise ValueError(f"Configuration data validation failed for {path}:\n{e}")
