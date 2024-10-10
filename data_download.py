import time
import os
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm

BASE_URL = "https://api.binance.com"
REQ_LIMIT = 1000
SUPPORT_INTERVAL = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}

# 设置代理
PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}


def get_support_symbols():
    """获取支持交易的交易对列表"""
    res = []
    end_point = "/api/v3/exchangeInfo"
    try:
        resp = requests.get(BASE_URL + end_point, timeout=10, proxies=PROXIES)  # 添加代理
        resp.raise_for_status()  # 检查请求是否成功
        for symbol_info in resp.json()["symbols"]:
            if symbol_info["status"] == "TRADING":
                symbol = "{}/{}".format(symbol_info["baseAsset"].upper(), symbol_info["quoteAsset"].upper())
                res.append(symbol)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching supported symbols: {e}")
    return res


def get_klines(symbol, interval='1h', since=None, limit=1000, to=None, retry_attempts=3):
    """从 Binance API 获取 K-line 数据"""
    end_point = "/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    if since:
        params['startTime'] = int(since * 1000)
    if to:
        params['endTime'] = int(to * 1000)

    for attempt in range(retry_attempts):
        try:
            # 添加代理
            resp = requests.get(BASE_URL + end_point, params=params, timeout=10, proxies=PROXIES)
            resp.raise_for_status()  # 检查请求状态码
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching klines data (attempt {attempt + 1}/{retry_attempts}): {e}")
            time.sleep(2)  # 请求失败后等待 2 秒重试
    return []


def download_full_klines(symbol, interval, start, end=None, save_to=None, req_interval=None, dimension="ohlcv"):
    """下载完整的 K-line 数据并保存为 CSV 文件"""
    if interval not in SUPPORT_INTERVAL:
        raise Exception(f"Interval {interval} is not supported!")

    start_end_pairs = get_start_end_pairs(start, end, interval)
    klines = []

    for (start_ts, end_ts) in tqdm(start_end_pairs, desc="Downloading Klines", unit="pair"):
        tmp_kline = get_klines(symbol.replace("/", ""), interval, since=start_ts, limit=REQ_LIMIT, to=end_ts)
        if len(tmp_kline) > 0:
            klines.append(tmp_kline)
        if req_interval:
            time.sleep(req_interval)

    klines = np.concatenate(klines)
    data = []
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "value", "trade_cnt", "active_buy_volume", "active_buy_value"]

    for i in range(len(klines)):
        tmp_kline = klines[i]
        data.append(tmp_kline[:-1])

    # 构建 DataFrame，使用 float 类型
    df = pd.DataFrame(np.array(data), columns=cols, dtype=float)

    # 处理无效值，将 NaN 替换为 0
    df.fillna(0, inplace=True)

    # 删除 "close_time" 列
    df.drop("close_time", axis=1, inplace=True)

    # 将特定列转换为整数类型
    for col in cols:
        if col in ["open_time", "trade_cnt"]:
            df[col] = df[col].astype(int, errors='ignore')  # 忽略不能转换为整数的值
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")

    # 只保留 OHLCV 数据
    if dimension == "ohlcv":
        df = df[cols[:6]]

    real_start = df["open_time"].iloc[0].strftime("%Y-%m-%d")
    real_end = df["open_time"].iloc[-1].strftime("%Y-%m-%d")

    # 保存为 CSV 文件
    if save_to:
        save_dir = os.path.dirname(save_to)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)  # 创建目录
        df.to_csv(save_to, index=False)
    else:
        df.to_csv(f"{symbol.replace('/', '-')}_{interval}_{real_start}_{real_end}.csv", index=False)


def get_start_end_pairs(start, end, interval):
    """获取开始和结束的时间戳对"""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    if end is None:
        end_dt = datetime.now()
    else:
        end_dt = datetime.strptime(end, "%Y-%m-%d")
    start_dt_ts = int(time.mktime(start_dt.timetuple()))
    end_dt_ts = int(time.mktime(end_dt.timetuple()))

    ts_interval = interval_to_seconds(interval)

    res = []
    cur_start = cur_end = start_dt_ts
    while cur_end < end_dt_ts - ts_interval:
        cur_end = min(end_dt_ts, cur_start + (REQ_LIMIT - 1) * ts_interval)
        res.append((cur_start, cur_end))
        cur_start = cur_end + ts_interval
    return res


def interval_to_seconds(interval):
    """将时间间隔转换为秒"""
    seconds_per_unit = {"m": 60, "h": 60 * 60, "d": 24 * 60 * 60, "w": 7 * 24 * 60 * 60}
    return int(interval[:-1]) * seconds_per_unit[interval[-1]]


if __name__ == '__main__':
    symbols = get_support_symbols()
    download_full_klines(symbol="BTC/USDT", interval="15m", start="2021-07-01", end="2021-08-01",
                         save_to="")
