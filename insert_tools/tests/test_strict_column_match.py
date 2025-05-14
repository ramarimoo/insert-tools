import pytest
from clickhouse_insert.runner import InsertConfig, run_insert, SchemaMismatchError
from unittest.mock import MagicMock, patch
import pandas as pd

def mock_client_with_extra_column():
    client = MagicMock()

    # Целевая таблица: только id и name
    client.query_df.side_effect = [
        pd.DataFrame({
            "name": ["id", "name"],
            "type": ["Int32", "String"]
        }),
        pd.DataFrame({
            "name": ["id", "name", "extra"],
            "type": ["Int32", "String", "String"]
        })
    ]

    return client

@patch("clickhouse_insert.runner.get_client", side_effect=lambda **kwargs: mock_client_with_extra_column())
def test_strict_column_mismatch_raises(mocked_client):
    cfg = InsertConfig(
        host="localhost",
        database="test",
        target_table="users",
        select_sql="SELECT id, name, extra FROM staging.tmp_users",
        allow_type_cast=False
    )
    cfg.strict_column_match = True

    with pytest.raises(SchemaMismatchError) as excinfo:
        run_insert(cfg)

    assert "STRICT" in str(excinfo.value)