import mysql.connector
import os

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "research_mysql2"
}

SQL_FILE = r"d:\Amrita\Research\DBMS\DBMS PROJECT\DBMS_Backend\scripts\create_users_table.sql"

def execute_sql_file():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        with open(SQL_FILE, 'r') as f:
            sql_script = f.read()
            
        # Split statements by semicolon and execute
        statements = sql_script.split(';')
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)
        
        conn.commit()
        print("✅ SQL script executed successfully")
        
    except mysql.connector.Error as err:
        print(f"❌ Error: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    execute_sql_file()
