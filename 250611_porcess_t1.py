# 文件名: async_download_with_token.py
# 最终优化版：结合了您编写的异步下载逻辑和我们获取到的 OAuth 2.0 Access Token，并进行了多项优化。

import asyncio
import os
import sys
import aiohttp
import csv
from pathlib import Path
from tqdm.asyncio import tqdm

# --- 配置参数 ---
API_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
TOTAL_ROWS_API_URL = "https://data.cityofchicago.org/resource/wrvz-psew.json?$select=count(trip_id)"
PAGE_LIMIT = 5000

# --- 【关键优化】并发与路径设置 ---
# 将并发数降低到一个服务器更能接受的水平，以避免403错误
MAX_CONCURRENT_REQUESTS = 5
SCRIPT_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()
OUTPUT_FILENAME = SCRIPT_DIR / "Chicago_taxi_trip_authenticated_full.csv"

# ---【关键优化】授权与请求头配置 ---
# 1. 从环境变量中读取您保存的 Access Token
ACCESS_TOKEN = os.getenv("MY_ACCESS_TOKEN")

# 2. 构造带有完整授权信息的 HTTP 请求头
#    - 检查 ACCESS_TOKEN 是否存在
#    - 加入 User-Agent，模拟真实浏览器，增加请求成功率
if not ACCESS_TOKEN:
    HEADERS = {}  # 留空，主程序会检查并报错
else:
    HEADERS = {
        'Accept': 'text/csv',
        'Authorization': f'OAuth {ACCESS_TOKEN}',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

async def fetch_total_rows(session):
    """获取总记录数"""
    try:
        async with session.get(TOTAL_ROWS_API_URL, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return int(data[0]['count_trip_id'])
    except Exception as e:
        print(f"获取总行数失败: {e}。将无法显示总进度。")
        return None

async def fetch_page(session, offset, writer, lock, semaphore, pbar):
    """使用信号量和授权Token，异步获取单个分页的数据"""
    async with semaphore:
        # 增加 $order 参数，可以提高数据库查询效率
        params = {'$limit': PAGE_LIMIT, '$offset': offset, '$order': ':id'}
        for attempt in range(3):
            try:
                async with session.get(API_URL, params=params, headers=HEADERS, timeout=60) as response:
                    response.raise_for_status()
                    content = await response.text()
                    lines = content.strip().splitlines()

                    if not lines or len(lines) <= 1:
                        pbar.update(1)
                        return 0

                    csv_reader = csv.reader(lines)
                    data_page = list(csv_reader)
                    
                    async with lock:
                        writer.writerows(data_page[1:])
                    
                    pbar.update(1)
                    return len(data_page) - 1

            except aiohttp.ClientResponseError as e:
                # 【关键优化】如果是401或403，说明Token或权限有问题，直接失败，不再重试
                if e.status in [401, 403]:
                    print(f"\n授权错误 (Offset: {offset}, Status: {e.status})。请检查您的 Access Token 是否正确或已过期。")
                    pbar.update(1)
                    return -1 # 返回错误
                else:
                    print(f"\n网络请求错误 (Offset: {offset}, Status: {e.status})。将在 {2 ** attempt} 秒后重试...")
                    await asyncio.sleep(2 ** attempt)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"\n网络连接或超时错误 (Offset: {offset}): {e}。将在 {2 ** attempt} 秒后重试...")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"\n处理数据时出错 (Offset: {offset}): {e}")
                pbar.update(1)
                return -1

        print(f"\n任务失败 (Offset: {offset})，已达到最大重试次数。")
        pbar.update(1)
        return -1

async def fetch_and_write_header(session):
    """单独获取并写入CSV表头"""
    params = {'$limit': 1}
    try:
        async with session.get(API_URL, params=params, headers=HEADERS) as response:
            response.raise_for_status()
            content = await response.text()
            lines = content.strip().splitlines()
            if lines:
                return lines[0].split(',')
    except Exception as e:
        print(f"获取表头失败: {e}")
        return None

async def main():
    """主执行函数,使用授权Token进行并发下载"""
    
    # 在程序开始时检查 Access Token 是否已设置
    if not ACCESS_TOKEN:
        print("错误：请先设置环境变量 MY_ACCESS_TOKEN", file=sys.stderr)
        print("运行命令: export MY_ACCESS_TOKEN='你获取到的访问令牌'", file=sys.stderr)
        sys.exit(1)

    print(">>> 采用授权并发模式开始下载...")
    print(f"文件将被保存在: {OUTPUT_FILENAME}")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    lock = asyncio.Lock()
    total_rows_written = 0

    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        total_rows = await fetch_total_rows(session)
        if total_rows is None:
            print("警告：无法确定下载总量，进度条可能不准确。")
            total_rows = 2000000 # 假设一个比较大的值

        header = await fetch_and_write_header(session)
        if not header:
            print("无法获取数据表头，程序退出。")
            return

        with open(OUTPUT_FILENAME, 'w', newline='', encoding='utf-8-sig') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)

            offsets_to_fetch = range(0, total_rows, PAGE_LIMIT)
            # 提醒：Socrata 不支持过大的 offset，这里限制在 50000 以内
            offsets_to_fetch = [o for o in offsets_to_fetch if o <= 50000] 
            
            # 如果没有可下载的批次，则退出
            if not offsets_to_fetch:
                print("没有可下载的数据（可能总行数小于分页大小）。")
                return

            tasks = []
            with tqdm(total=len(offsets_to_fetch), desc="分页下载进度", unit="页") as pbar:
                for offset in offsets_to_fetch:
                    task = asyncio.create_task(fetch_page(session, offset, writer, lock, semaphore, pbar))
                    tasks.append(task)
                
                batch_results = await asyncio.gather(*tasks)

            successful_results = [r for r in batch_results if r > 0]
            total_rows_written = sum(successful_results)

    print(f"\n\n处理完成! 数据已保存到 {OUTPUT_FILENAME}")
    print(f"总共成功写入了 {total_rows_written} 条记录。")

if __name__ == "__main__":
    asyncio.run(main())