from clickhouse_insert.cast_rewriter import rewrite_select_with_cast
import pytest

@pytest.mark.parametrize("source_schema, target_schema, select_sql, expected", [
    # простая замена одного поля
    (
            {"name": ("String", False), "id": ("String", False)},
            {"id": ("Int64", False), "name": ("String", False)},
            "SELECT name, id FROM staging.tmp_users",
            "SELECT name, CAST(id AS Int64) AS id FROM staging.tmp_users"
    ),
    # оба поля требуют CAST
    (
            {"id": ("String", False), "amount": ("Float32", False)},
            {"id": ("UInt32", False), "amount": ("Float64", False)},
            "SELECT id, amount FROM staging.tmp_data",
            "SELECT CAST(id AS UInt32) AS id, CAST(amount AS Float64) AS amount FROM staging.tmp_data"
    ),
    # нет преобразований, типы совпадают
    (
            {"id": ("UInt32", False), "name": ("String", False)},
            {"id": ("UInt32", False), "name": ("String", False)},
            "SELECT id, name FROM users",
            "SELECT id, name FROM users"
    ),
    # порядок в SELECT важен, только одно поле требует CAST
    (
            {"amount": ("String", False), "id": ("UInt64", False)},
            {"amount": ("Float64", False), "id": ("UInt64", False)},
            "SELECT amount, id FROM payments",
            "SELECT CAST(amount AS Float64) AS amount, id FROM payments"
    ),
])
def test_cast_variants(source_schema, target_schema, select_sql, expected):
    rewritten = rewrite_select_with_cast(select_sql, source_schema, target_schema)
    assert rewritten.replace(" ", "") == expected.replace(" ", "")