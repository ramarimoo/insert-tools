import argparse
import logging
from clickhouse_insert.runner import InsertConfig, run_insert, SchemaMismatchError

def main():
    # Создаем парсер командной строки
    parser = argparse.ArgumentParser(description="ClickHouse Insert CLI Tool")

    from clickhouse_insert import __version__
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument("--host", type=str, help="Host of the ClickHouse server", required=False)
    parser.add_argument("--port", type=int, default=8123, help="Port of the ClickHouse server", required=False)
    parser.add_argument("--user", type=str, help="User for ClickHouse authentication", required=False)
    parser.add_argument("--password", type=str, help="Password for ClickHouse authentication", required=False)
    parser.add_argument("--database", type=str, help="Target database in ClickHouse", required=False)
    parser.add_argument("--target_table", type=str, help="Target table in ClickHouse", required=False)
    parser.add_argument("--select_sql", type=str, help="SELECT query to fetch data", required=False)
    parser.add_argument("--allow_type_cast", action="store_true", help="Allow type casting if needed", required=False)

    # Убираем обязательный параметр config для использования параметров CLI напрямую
    # parser.add_argument("--config", required=True, help="Path to config file (.json/.yaml/.env)")

    parser.add_argument("--dry-run", action="store_true", help="Только проверить схему, не вставлять данные")
    parser.add_argument("--strict", action="store_true", help="Ошибка при наличии лишних колонок в SELECT")
    parser.add_argument("--verbose", action="store_true", help="Подробный лог (DEBUG)")
    parser.add_argument("--insert-columns", type=str, help="Список колонок для INSERT через запятую (в порядке)")

    # Получаем аргументы
    args = parser.parse_args()

    # Включаем подробные логи, если нужно
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        print("[VERBOSE] Включён режим DEBUG")

    try:
        # Формируем словарь конфигурации
        config_params = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "database": args.database,
            "target_table": args.target_table,
            "select_sql": args.select_sql,
            "allow_type_cast": args.allow_type_cast
        }

        # Пропускаем пустые параметры
        config_params = {key: value for key, value in config_params.items() if value is not None}

        # Создаем объект конфигурации
        config = InsertConfig(**config_params)

        # Обрабатываем dry-run
        if args.dry_run:
            print("[DRY-RUN] Проверка схемы без вставки")
            config._dry_run = True

        # Включаем строгую проверку колонок, если требуется
        if args.strict:
            print("[STRICT] Проверка: лишние поля вызовут ошибку")
            config.strict_column_match = True

        # Обрабатываем аргумент insert-columns
        if args.insert_columns:
            config.insert_columns = [col.strip() for col in args.insert_columns.split(",")]
            print(f"[COLUMNS] Вставка только в колонки: {config.insert_columns}")

        # Выполняем вставку данных
        run_insert(config)

        if args.verbose or args.dry_run:
            logging.debug(f"Итоговый SQL: {config.select_sql}")

    except SchemaMismatchError as e:
        print(e)
        exit(1)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        exit(2)

if __name__ == "__main__":
    main()
