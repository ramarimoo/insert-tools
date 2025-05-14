from clickhouse_connect import get_client
from clickhouse_insert import InsertConfig, run_insert

def test_insert_against_live_clickhouse():
    client = get_client(host="localhost", port=8123, user="default", password="admin123")

    # Подготовка: пересоздаём таблицу
    client.command("DROP TABLE IF EXISTS test_insert")
    client.command("CREATE TABLE test_insert (id UInt32, name String) ENGINE = MergeTree() ORDER BY id")

    # Начальные данные
    client.command("INSERT INTO test_insert VALUES (1, 'Alice'), (2, 'Bob')")

    # Настройка и запуск вставки
    cfg = InsertConfig(
        host="localhost",
        port=8123,
        user="default",
        password="admin123",
        database="default",
        target_table="test_insert",
        select_sql="SELECT * FROM test_insert",
        allow_type_cast=True
    )

    run_insert(cfg)

    # Проверка: теперь должно быть 4 строки
    result = client.query("SELECT count() FROM test_insert").result_rows
    assert result[0][0] == 4, f"Expected 4 rows after insert, got: {result[0][0]}"
