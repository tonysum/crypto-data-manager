import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
db_path = "~/downloads/nan/crypto_data.db"

#连接数据库
def connect_db(path):
    expanded_path = os.path.expanduser(path)
    if not os.path.exists(expanded_path):
        print(f"❌ 错误: 文件不存在: {expanded_path}")
        sys.exit(1)
    conn = sqlite3.connect(expanded_path)
    return conn

#获取所有表名
def get_all_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

print("连接数据库...")
print(f"数据库路径: {db_path}")
print("-" * 50)
print("获取所有表名...")
print("-" * 50)
print(len(get_all_tables(connect_db(db_path))))

#获取第一个表的数据
def get_data(table_name):
    