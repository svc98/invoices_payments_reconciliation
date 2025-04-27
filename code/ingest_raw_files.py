##### Main Functions #####
def check_or_create_tables(conn, cursor, TABLE_SETUP_PATH, expected_tables):
    # Iterate through SQLite tables and grab table name
    existing_tables = [row[0] for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")]
    check = all(table.lower() in existing_tables for table in expected_tables)

    # If all values in check output to True then skip, else create tables.
    if not check:
        print("Schema missing, initializing...")
        with open(TABLE_SETUP_PATH, "r") as f:
            conn.executescript(f.read())
        print("Schema created.")
    else:
        print("Schema already exists. Skipping creation.")
