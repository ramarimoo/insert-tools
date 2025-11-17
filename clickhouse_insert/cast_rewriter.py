
import re
from typing import Dict, Tuple

def rewrite_select_with_cast(select_sql: str,
                              source_schema: Dict[str, Tuple[str, bool]],
                              target_schema: Dict[str, Tuple[str, bool]]) -> str:
    """
    Переписывает SELECT-запрос с добавлением CAST(...) там, где типы отличаются и безопасное приведение допустимо.
    """
    new_columns = []
    for col_name, (src_type, _) in source_schema.items():
        tgt_type, _ = target_schema.get(col_name, (src_type, False))

        if src_type != tgt_type:
            # CAST(col_name AS target_type) AS col_name
            new_columns.append(f"CAST({col_name} AS {tgt_type}) AS {col_name}")
        else:
            # if =type just add column
            new_columns.append(col_name)

    # if schema is empty (even if it is not logical), do not return empty list
    if not new_columns:
        raise ValueError("source_schema empty. New SELECT list cannot be created.")

    match = re.search(r"FROM\s+.*", select_sql, re.IGNORECASE | re.DOTALL)
    
    if not match:
        raise ValueError("SELECT-Query not splitted: 'FROM' key word couldn't find.")

    from_part_and_rest = match.group(0).strip()
    
    return f"SELECT {', '.join(new_columns)} {from_part_and_rest}"
