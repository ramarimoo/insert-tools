import pytest
from clickhouse_connect import get_client
import socket
from clickhouse_insert import run_insert, InsertConfig
import os
import time
import requests

def wait_for_clickhouse_ready(host='localhost', port=8123, timeout=30):
    url = f'http://{host}:{port}/ping'
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    raise RuntimeError("ClickHouse did not become ready in time")

@pytest.fixture(scope="session")
def docker_compose_file():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'docker-compose.yml'))

def test_insert_into_clickhouse(docker_services):
    wait_for_clickhouse_ready(port=18123)

    client = get_client(host='localhost', port=18123, username='default', password='admin123')
    client.command('CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = MergeTree() ORDER BY id')

    cfg = InsertConfig(
        host="localhost",
        port=18123,
        database="default",
        target_table="test_table",
        select_sql="SELECT toUInt32(1) AS id, 'John' AS name",
        password="admin123",
    )

    run_insert(cfg)

    result = client.query('SELECT id, name FROM test_table').result_rows
    assert result == [(1, 'John')], f"Expected [(1, 'John')], but got {result}"
