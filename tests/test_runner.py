import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from clickhouse_insert.runner import InsertConfig, run_insert

@patch("clickhouse_insert.runner.get_client")
def test_insert_with_type_cast(mock_get_client):
    client = MagicMock()

    # Целевая схема
    client.query_df.side_effect = [
        pd.DataFrame({
            "name": ["id", "amount"],
            "type": ["Int64", "Float64"]
        }),
        pd.DataFrame({
            "name": ["id", "amount"],
            "type": ["String", "Float32"]
        })
    ]

    client.command = MagicMock()
    mock_get_client.return_value = client

    cfg = InsertConfig(
        host="localhost",
        database="test",
        target_table="payments",
        select_sql="SELECT id, amount FROM staging.tmp_payments",
        allow_type_cast=True
    )

    run_insert(cfg)

    client.command.assert_called()
    sql = client.command.call_args[0][0]
    assert "CAST(id AS Int64) AS id" in sql
    assert "CAST(amount AS Float64) AS amount" in sql
