import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

def check_sqlite_integrity(db_path):
    """
    对 SQLite 数据库进行完整性校验
    1. 数据库文件级别检查 (PRAGMA integrity_check)
    2. 业务数据级别检查 (K线连续性)
    """
    if not os.path.exists(db_path):
        print(f"❌ 错误: 文件不存在: {db_path}")
        return

    print(f"🔍 开始校验数据库: {db_path}")
    print(f"文件大小: {os.path.getsize(db_path) / (1024*1024):.2f} MB")
    print("-" * 50)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. 数据库底层完整性检查
        print("1. 执行数据库底层校验 (PRAGMA integrity_check)...")
        cursor.execute("PRAGMA integrity_check;")
        result = cursor.fetchone()
        if result[0] == "ok":
            print("   ✅ 数据库底层结构正常 (OK)")
        else:
            print(f"   ❌ 数据库结构损坏: {result[0]}")
            conn.close()
            return

        # 2. 获取所有表并检查数据
        print("\n2. 执行业务数据连续性校验...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'K%';")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("   ⚠️  未发现 K 线数据表 (表名需以 K 开头，如 K1dBTCUSDT)")
            conn.close()
            return

        print(f"   待检查表数量: {len(tables)}")
        
        for table in tables:
            print(f"\n   分析表: {table} ...")
            try:
                # 获取该表的时间范围和总行数
                df = pd.read_sql_query(f"SELECT trade_date FROM \"{table}\" ORDER BY trade_date ASC", conn)
                
                if df.empty:
                    print("      ⚠️  表为空")
                    continue
                
                # 转换日期
                df['date'] = pd.to_datetime(df['trade_date'].str[:10])
                count = len(df)
                start_date = df['date'].min()
                end_date = df['date'].max()
                
                # 计算预期天数 (假设是日线，如果是其他周期需调整逻辑)
                # 简单判断周期
                if "1d" in table:
                    expected_days = (end_date - start_date).days + 1
                    missing = expected_days - count
                    
                    print(f"      记录数: {count}")
                    print(f"      范围: {start_date.date()} -> {end_date.date()}")
                    
                    if missing <= 0:
                        print("      ✅ 数据连续性: 完整")
                    else:
                        print(f"      ❌ 数据缺失: 预计 {expected_days} 天, 实际 {count} 天, 缺失 {missing} 天")
                        
                        # 找出具体缺失日期 (示例)
                        all_dates = pd.date_range(start=start_date, end=end_date)
                        missing_dates = all_dates.difference(df['date'])
                        if len(missing_dates) > 0:
                            print(f"      具体缺失示例: {list(missing_dates[:5])}")
                else:
                    print(f"      记录数: {count} (非 1d 周期，跳过连续性精确计算)")
                    
            except Exception as e:
                print(f"      ❌ 检查表 {table} 出错: {e}")

        conn.close()
        print("\n" + "="*50)
        print("校验完成")

    except Exception as e:
        print(f"❌ 发生异常: {e}")

if __name__ == "__main__":
    # 使用用户提供的路径，如果没提供则使用默认db
    target_path = sys.argv[1] if len(sys.argv) > 1 else "/downloads/nan/crypto_data.db"
    check_sqlite_integrity(target_path)
