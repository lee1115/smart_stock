import akshare as ak
import pandas as pd
import time
import argparse
import os
import joblib  # 或 import pickle as joblib (如果用pickle)
from datetime import datetime, timedelta

CACHE_DIR = "cache"
CACHE_FILE = os.path.join(CACHE_DIR, "stock_data.pkl")
CACHE_EXPIRY_HOURS = 1  # 缓存过期时间（小时）

def load_or_fetch_data():
    os.makedirs(CACHE_DIR, exist_ok=True)  # 创建缓存目录

    # 检查缓存是否存在且未过期
    if os.path.exists(CACHE_FILE):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
        if datetime.now() - file_mod_time < timedelta(hours=CACHE_EXPIRY_HOURS):
            try:
                return joblib.load(CACHE_FILE)
            except Exception as e:
                print(f"缓存加载失败: {e}，重新查询...")
     # 否则，查询新数据
    print("查询新数据...")
    df = ak.stock_zh_a_spot_em()
    # 保存到缓存
    joblib.dump(df, CACHE_FILE)
    print("数据已缓存。")
    return df

def add_exchange_suffix(code: str) -> str:
    """
    输入 6 位股票代码字符串，返回带交易所后缀的股票代码
    例如：
        "301389" -> "301389.SZ"
        "688001" -> "688001.SH"
    """
    if not isinstance(code, str) or not code.isdigit():
        raise ValueError("股票代码必须是数字字符串")

    if len(code) != 6:
        raise ValueError("股票代码长度必须为 6 位")

    # 上交所（含科创板）
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"{code}.SH"

    # 深交所
    if code.startswith(("000", "001", "002", "300", "301")):
        return f"{code}.SZ"

    # 北交所
    if code.startswith((
        "430", "831", "832", "833", "834",
        "835", "836", "837", "838"
    )):
        return f"{code}.BJ"

    # 未识别的情况
    raise ValueError(f"无法识别股票代码所属板块: {code}")

def analyse_stocks(symb,indic='按单季度',min_roe=15, min_gross_margin=40,min_net_margin=5,max_pe=20,max_pb=10,max_de=0.5):
    df_fin=ak.stock_financial_analysis_indicator_em(symb,indic)
    latest =df_fin.iloc[0] #最新一期数据
    roe = latest['ROEKCJQ'] if 'ROE' in latest else 0
    gross_margin = latest['XSMLL'] if 'XSMLL' in latest else 0
    net_margin = latest['XSJLL'] if 'XSJLL' in latest else 0
    de = latest['ASSETLIABRATIO'] / (100 - latest['ASSETLIABRATIO']) if 'ASSETLIABRATIO' in latest else 999  # 近似D/E = 负债/权益

    return 



def select_stocks(min_market_cap=10000000000, max_pe=20, max_pb=10,min_roe=15,top_n=100):
    df = load_or_fetch_data()  # 使用缓存函数
    filtered = df[(df['总市值'] > min_market_cap) &
                  (df['市盈率-动态'] < max_pe) &
                  (df['市净率']< max_pb)
 #                & (df['roe'] > min_roe)
                ]
    sorted_df = filtered.sort_values(by='总市值', ascending=False).head(top_n)
    return sorted_df


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="自定义选股工具（带缓存）")
    parser.add_argument("--min_cap", type=float, default=10000000000, help="最小市值（元）")
    parser.add_argument("--max_pe", type=float, default=20, help="最大PE")
    parser.add_argument("--max_pb",type=float,default=10,help="最大PB")
    parser.add_argument("--min_roe", type=float, default=15, help="最小ROE（%）")
    parser.add_argument("--top", type=int, default=100, help="输出前N只")
    args = parser.parse_args()

 #   result = select_stocks(args.min_cap, args.max_pe, args.max_pb, args.min_roe, args.top)
 #   result.to_csv("selected_stocks.csv", index=False)  # 导出CSV
 
    result = ak.stock_financial_analysis_indicator_em('301389.SZ','按单季度')

    roe = result['ROE_DILUTED'] if 'ROE_DILUTED' in result else 0
    gross_margin = result['GROSS_PROFIT_RATIO'] if 'GROSS_PROFIT_RATIO' in result else 0
    net_margin = result['NET_PROFIT_RATIO'] if 'NET_PROFIT_RATIO' in result else 0



    result.to_csv("test_stocks.csv", index=False)  # 导出CSV
    print(f"roe={roe}")
    print(f"gross_margin={gross_margin}%")
    print(f"net_margin={net_margin}%")
    print(result)