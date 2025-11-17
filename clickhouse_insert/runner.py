import uuid
import pandas as pd
import logging
from clickhouse_connect import get_client
from .cast_rewriter import rewrite_select_with_cast

# Логгер
logger = logging.getLogger("insert_tool")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler("insert.log", encoding="utf-8")
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


class InsertConfig:
    def __init__(self, host, database, target_table, select_sql,
                 user=None, password=None, port=8123, allow_type_cast=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.target_table = target_table
        self.select_sql = select_sql
        self.allow_type_cast = allow_type_cast
        self._dry_run = False
        self.strict_column_match = False
        self.insert_columns = None


class SchemaMismatchError(Exception):
    pass


def parse_type(type_str):
    if type_str.startswith("Nullable("):
        return type_str[9:-1], True
    return type_str, False


def is_cast_compatible(source_type, target_type):
    compatible_casts = {
        "String": {"UInt32", "UInt64", "Int32", "Int64", "Float32", "Float64"},
        "Int32": {"UInt32", "Float64"}, # Turning into UInt32 can cause float.
        "Int64": {"UInt64", "Float64"},
        "Float32": {"Float64"},
    }
    return target_type in compatible_casts.get(source_type, set())


def _get_schema(client, source, is_subquery=False):
    if is_subquery:
        query = f"DESCRIBE TABLE ({source} LIMIT 0)"
    else:
        query = f"DESCRIBE TABLE {source}"
    
    try:
        df = client.query_df(query)
    except Exception as e:
        logger.error(f"Şema alınırken hata oluştu (Kaynak: {'Sorgu' if is_subquery else source}): {e}")
        raise ValueError(f"Şema sorgusu başarısız oldu: {e}")

    return {row["name"]: parse_type(row["type"]) for _, row in df.iterrows()}

def _validate_schemas(select_schema, target_schema, cfg):

    errors = []

    for col, (sel_type, sel_nullable) in select_schema.items():
        if col not in target_schema:
            if cfg.strict_column_match:
                errors.append(f"\n[ERROR] STRICT: Колонка '{col}' есть в SELECT, но отсутствует в целевой таблице")
            continue

        tgt_type, tgt_nullable = target_schema[col]

        if sel_type == tgt_type:
            pass # = types
        elif cfg.allow_type_cast and is_cast_compatible(sel_type, tgt_type):
            logger.warning(f"[WARN] Приведение типа: {col} из {sel_type} в {tgt_type}")
        else:
            errors.append(f"\n[ERROR] Колонка '{col}': тип SELECT '{sel_type}' != тип TARGET '{tgt_type}'")

        if sel_nullable and not tgt_nullable:
            errors.append(f"\n[ERROR] Колонка '{col}': SELECT может содержать NULL, но целевая таблица — NOT NULL")

    if cfg.strict_column_match:
        select_cols = set(select_schema.keys())
        target_cols = set(target_schema.keys())
        missing_cols = target_cols - select_cols
        
        if missing_cols:
            errors.append(f"\n[ERROR] STRICT: Отсутствуют целевые колонки в SELECT: {', '.join(missing_cols)}")

    if errors:
        raise SchemaMismatchError("Ошибка соответствия схемы:" + "".join(errors))


def run_insert(cfg: InsertConfig):
    
    ry:
        client = get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.user,
            password=cfg.password,
            database=cfg.database
        )
    except Exception as e:
        logger.error(f"Не удалось установить соединение с ClickHouse: {e}")
        raise

    logger.info("Получаем схему целевой таблицы...")
    target_schema = _get_schema(client, f"{cfg.database}.{cfg.target_table}")

    logger.info("Получаем схему запроса...")
    select_schema = _get_schema(client, cfg.select_sql, is_subquery=True)

    if not select_schema:
        raise ValueError("SELECT-запрос не вернул колонок. Возможно ошибка в SQL.")

    _validate_schemas(select_schema, target_schema, cfg)

    if cfg.allow_type_cast:
        cfg.select_sql = rewrite_select_with_cast(cfg.select_sql, select_schema, target_schema)
        logger.debug(f"Финальный SELECT после CAST: {cfg.select_sql}")

    if getattr(cfg, "_dry_run", False):
        logger.info(f"[DRY-RUN] Вставка пропущена. SQL: {cfg.select_sql}")
        return

    insert_cols = f"({', '.join(cfg.insert_columns)})" if cfg.insert_columns else ""
    logger.info("Схемы совместимы. Выполняем вставку...")

    final_sql = f"INSERT INTO `{cfg.database}`.`{cfg.target_table}` {insert_cols} {cfg.select_sql}"
    logger.debug(f"INSERT SQL: {final_sql}")

    client.command(final_sql)
