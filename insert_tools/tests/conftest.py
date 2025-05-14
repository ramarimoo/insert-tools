import pytest
import datetime
from clickhouse_connect import get_client
from clickhouse_insert import InsertConfig, run_insert
from clickhouse_connect.driver.exceptions import DatabaseError


@pytest.fixture(scope="module")
def ch_client():
    client = get_client(host="localhost", port=8123, user="default", password="admin123")
    yield client
    client.close()


def create_table(client, schema: str):
    client.command("DROP TABLE IF EXISTS test_case")
    client.command(f"CREATE TABLE test_case ({schema}) ENGINE = MergeTree() ORDER BY tuple()")


def insert_rows(client, rows):
    if not rows:
        return
    values = []
    for row in rows:
        formatted = []
        for v in row:
            if v is None:
                formatted.append("NULL")
            elif isinstance(v, str):
                formatted.append(f"'{v}'")
            elif isinstance(v, datetime.date):
                formatted.append(f"'{v.isoformat()}'")
            else:
                formatted.append(str(v))
        values.append(f"({', '.join(formatted)})")
    client.command(f"INSERT INTO test_case VALUES {', '.join(values)}")


def fetch_all_rows(client):
    return client.query("SELECT * FROM test_case").result_rows


@pytest.mark.parametrize("source_data, table_schema, allow_type_cast, should_fail", [
    ([(1, 'Alice')], "id UInt32, name String", True, False),
    ([(1,)], "id UInt32", True, False),
    ([], "id UInt32, name String", True, False),
    ([(1, '42')], "id UInt32, name UInt32", True, False),
    ([(1, 'Bob')], "id UInt32, name UInt32", False, True),
    ([(1, '2024-01-01')], "id UInt32, created Date", True, False),
    ([(1, 3.14)], "id UInt32, value Float64", True, False),
    ([(1, 'wrong-date')], "id UInt32, created Date", True, True),
    ([(1,)], "id UInt32, name String", True, True),
    ([(1, None)], "id UInt32, name Nullable(String)", True, False),
    ([(None, 'John')], "id Nullable(UInt32), name String", True, False),
    ([(1, 'abc')], "id UInt32, name UInt32", False, True),
    # New conversion test case: numeric column receives string, allow_type_cast enabled
    ([(1, '123'), (2, '456')], "id UInt32, value UInt32", True, False),
    ([(1, 'bad')], "id UInt32, value UInt32", True, True),
])
def test_insert_case_variants(ch_client, source_data, table_schema, allow_type_cast, should_fail):
    create_table(ch_client, table_schema)

    cfg = InsertConfig(
        host="localhost",
        port=8123,
        user="default",
        password="admin123",
        database="default",
        target_table="test_case",
        select_sql="SELECT * FROM test_case",
        allow_type_cast=allow_type_cast
    )

    if should_fail:
        with pytest.raises(Exception):
            if source_data:
                insert_rows(ch_client, source_data)
            run_insert(cfg)
    else:
        insert_rows(ch_client, source_data)
        run_insert(cfg)

        inserted = fetch_all_rows(ch_client)

        def normalize(val):
            if isinstance(val, datetime.date):
                return val.isoformat()
            if isinstance(val, (int, float)):
                return val
            try:
                return int(val)
            except (ValueError, TypeError):
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return val

        normalized = [tuple(normalize(v) for v in row) for row in inserted]
        expected = [tuple(normalize(v) for v in row) for row in source_data]

        for row in expected:
            assert row in normalized


@pytest.mark.parametrize("select_sql, expected, params", [
    ("SELECT * FROM source", "SELECT * FROM source", {}),
    ("SELECT * FROM source WHERE id > {min_id}", "SELECT * FROM source WHERE id > 100", {"min_id": 100}),
    ("SELECT id, name FROM source WHERE name = '{name}'", "SELECT id, name FROM source WHERE name = 'Alice'", {"name": "Alice"}),
])
def test_build_select_query_formatting(select_sql, expected, params):
    result_query = select_sql.format(**params) if params else select_sql
    assert result_query == expected


def test_get_table_schema_and_validation(ch_client):
    create_table(ch_client, "id UInt32, name String")
    result = ch_client.query("DESC test_case").result_rows
    normalized = [(row[0], row[1].split('(')[0]) for row in result]
    assert normalized == [("id", "UInt32"), ("name", "String")]
