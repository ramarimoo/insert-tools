
import re
from typing import Dict, Tuple

def rewrite_select_with_cast(select_sql: str,
                              source_schema: Dict[str, Tuple[str, bool]],
                              target_schema: Dict[str, Tuple[str, bool]]) -> str:
    """
    Переписывает SELECT-запрос с добавлением CAST(...) там, где типы отличаются и безопасное приведение допустимо.
    """
    columns = []
    for col, (src_type, _) in source_schema.items():
        tgt_type, _ = target_schema.get(col, (src_type, False))
        if src_type != tgt_type:
            columns.append(f"CAST({col} AS {tgt_type}) AS {col}")
        else:
            columns.append(col)

    # Пытаемся заменить только список колонок в SELECT (не FROM, не WHERE)
    match = re.match(r"\s*SELECT\s+(.+?)\s+FROM\s+(.+)", select_sql, re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError("Не удалось разобрать SELECT-запрос")

    col_part, from_part = match.groups()
    return f"SELECT {', '.join(columns)} FROM {from_part.strip()}"
