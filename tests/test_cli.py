import subprocess
import sys
import pytest
from clickhouse_connect import get_client

@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    """Создаёт базу и таблицу для теста CLI."""
    client = get_client(host="localhost", port=8123, username="default", password="admin123")
    client.command("CREATE DATABASE IF NOT EXISTS test")
    client.command("""
        CREATE TABLE IF NOT EXISTS test.my_table (
            id UInt32,
            name String
        ) ENGINE = MergeTree() ORDER BY id
    """)

def test_cli_runs():
    command = [
        sys.executable, '-m', 'clickhouse_insert.cli',
        '--host', 'localhost',
        '--user', 'default',
        '--password', 'admin123',
        '--database', 'test',
        '--target_table', 'my_table',
        '--select_sql', "SELECT toUInt32(1) AS id, 'test' AS name FROM system.one",
        '--allow_type_cast',
        '--insert-columns', 'id,name',
        '--dry-run',
        '--verbose'
    ]
    result = subprocess.run(command, capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    assert result.returncode == 0, f"CLI завершился с ошибкой: {result.stderr}"
    assert (
            "Вставка пропущена" in result.stdout
            or "Вставка пропущена" in result.stderr
    ), "Ожидали лог о dry-run вставке"
