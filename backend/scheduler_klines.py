import asyncio
import logging
from datetime import datetime
import sys
from pathlib import Path

# 获取当前文件的绝对路径，并将项目根目录添加到 sys.path
current_file = Path(__file__).resolve()
backend_dir = current_file.parent
project_root = backend_dir.parent
sys.path.insert(0, str(backend_dir))

# 导入下载逻辑
try:
    from download_klines import auto_update_all_symbols
    from download_other import AutoDataDownloader
except ImportError as e:
    print(f"导入失败: {e}. 请确保在 backend 目录下或正确设置了 Python 路径。")
    sys.exit(1)

# 配置日志
log_dir = project_root / "data"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "scheduler.log"

# 强制配置日志，即使已经配置过
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)

# 调度配置：间隔时间（分钟）
SCHEDULE_CONFIG = {
    "1d": 1440,
    "4h": 240,
    "1h": 60,
    "15m": 15,
    "5m": 5
}

# 异步任务队列
task_queue = asyncio.Queue()
# 记录当前排队中或正在运行的任务，防止重复
active_intervals = set()

async def run_task(interval: str):
    """执行指定的下载任务"""
    try:
        logging.info(f"🚀 [任务启动] 开始处理间隔: {interval}")
        
        # � 优化下载速度：使用并行下载并减小延迟
        # 之前的极限制速 (request_delay=3.0s, batch_delay=30.0s) 太慢
        # 🛡️ 极端安全配置：减少并发，大幅增加延迟
        # 调整为：2个线程，每个请求后休息 1.0s，每批次后休息 20s
        stats = await asyncio.to_thread(
            auto_update_all_symbols, 
            interval=interval,
            request_delay=1.0, 
            batch_delay=20.0,
            max_workers=2
        )
        
        if stats:
            success = stats.get('updated', 0) + stats.get('new', 0)
            no_need = stats.get('no_data_needed', 0)
            failed = stats.get('failed', 0)
            logging.info(f"✅ [任务完成] {interval}: 成功={success}, 无需更新={no_need}, 失败={failed}")
            
            # 🚑 自动检测封禁：如果全部失败且有错误信息，可能被封了
            if failed > 10 and success == 0:
                logging.error("‼️ 检测到大量失败，可能触发了 IP 封禁。系统将静默 20 分钟...")
                await asyncio.sleep(1200)
        else:
            logging.info(f"✅ [任务完成] {interval}")

        # 🔗 [新增集成] 如果是 1h 或 1d 周期，顺便更新其他数据（持仓、资金费率、基差等）
        if interval in ["1h", "1d"]:
            logging.info(f"🔗 [额外任务] {interval} 周期触发，开始更新其他数据 (资金费率、持仓比例、Premium Index 等)...")
            try:
                # 创建下载器实例 (默认下载所有类型)
                other_downloader = AutoDataDownloader()
                # 在单独的线程中运行同步的下载逻辑
                await asyncio.to_thread(other_downloader.run_once)
                logging.info(f"✅ [额外任务完成] 其他数据更新成功")
            except Exception as e:
                logging.error(f"❌ [额外任务异常] 其他数据更新失败: {e}")
            
    except Exception as e:
        logging.error(f"❌ [任务异常] {interval} 出错: {e}")
        # 如果是因为封禁报错，也休眠一会儿
        if "Way too many requests" in str(e) or "-1003" in str(e):
            logging.error("‼️ 确认为 API 封禁错误。系统进入 10 分钟冷却期...")
            await asyncio.sleep(600)
    finally:
        active_intervals.discard(interval)

async def worker():
    """消费者：从队列中取任务并顺序执行"""
    logging.info("👷 Worker 线程已启动，准备处理任务队列...")
    while True:
        # 获取一个任务
        interval = await task_queue.get()
        try:
            await run_task(interval)
        finally:
            # 标记任务完成
            task_queue.task_done()

async def scheduler_loop():
    """生产者：每分钟看表，按需投递任务到队列"""
    logging.info("=" * 50)
    logging.info("⏰ K线数据串行调度器已启动")
    logging.info(f"监控间隔: {list(SCHEDULE_CONFIG.keys())}")
    logging.info(f"模式: 串行排队 (保证同一时间只有一个下载任务)")
    logging.info("=" * 50)
    
    # 启动即刻尝试一次 5m 更新
    if "5m" not in active_intervals:
        active_intervals.add("5m")
        await task_queue.put("5m")
    
    while True:
        try:
            now = datetime.now()
            current_total_minutes = now.hour * 60 + now.minute
            
            # 检查每个配置的时间周期
            for interval_name, interval_mins in SCHEDULE_CONFIG.items():
                if current_total_minutes % interval_mins == 0:
                    # 1d 任务只在 0 点处理
                    if interval_name == "1d" and now.hour != 0:
                        continue
                    
                    # 如果该间隔已经在队列中，就跳过，避免积压
                    if interval_name not in active_intervals:
                        logging.info(f"📥 [排队中] 时间已到，将 {interval_name} 加入队列")
                        active_intervals.add(interval_name)
                        await task_queue.put(interval_name)
                    else:
                        logging.warning(f"⏳ [跳过] {interval_name} 已经在处理中，跳过本次触发")
            
            # 等待到下一分钟开始
            now = datetime.now()
            sleep_seconds = 61 - now.second
            await asyncio.sleep(sleep_seconds)
            
        except Exception as e:
            logging.error(f"调度主循环异常: {e}")
            await asyncio.sleep(60)

async def main():
    # 同时启动生产者和消费者
    await asyncio.gather(
        worker(),
        scheduler_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n🛑 用户中断，程序退出")
    except Exception as e:
        logging.error(f"致命错误: {e}")
