import json
import os
import sqlite3
from pathlib import Path

source = Path(Path(__name__).absolute()).parents[1] / 'import_to_pkg_maps'

# Connect to SQLite database
conn = sqlite3.connect('example2.db')
c = conn.cursor()

# Create packages and package_contents tables
c.execute('''
    CREATE TABLE packages (
        package_id INTEGER PRIMARY KEY,
        package_name TEXT UNIQUE
    )
''')
c.execute('''
    CREATE TABLE package_contents (
        content_id INTEGER PRIMARY KEY,
        package_id INTEGER,
        field_name TEXT,
        field_value TEXT,
        FOREIGN KEY (package_id) REFERENCES packages (package_id)
    )
''')

# Loop over files in directory
for filename in os.listdir(source):
    if filename.endswith('.json'):
        # Read file contents as JSON
        with open(os.path.join(source, filename), 'r') as f:
            data = json.load(f)

        # Extract package_name and fields from JSON
        package_name = filename.split('.json')[0]  # remove ".json" extension
        fields = []
        for field_name, value in data.items():
            field_value = value['elements'][0]
            fields.append((field_name, field_value))

        # Insert into packages table
        c.execute('''
            INSERT OR IGNORE INTO packages (package_name)
            VALUES (?)
        ''', (package_name,))
        package_id = c.lastrowid  # get package_id of inserted row

        # Insert into package_contents table
        for field in fields:
            c.execute('''
                INSERT INTO package_contents (package_id, field_name, field_value)
                VALUES (?, ?, ?)
            ''', (package_id, field[0], field[1]))

# Create indexes for package_id and package_name columns for faster lookup
c.execute('''
    CREATE INDEX idx_packages_package_id
    ON packages (package_id)
''')
c.execute('''
    CREATE INDEX idx_packages_package_name
    ON packages (package_name)
''')

# Commit changes and close connection
conn.commit()
conn.close()