import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytdx.hq import TdxHq_API
import logging
from typing import List, Dict, Tuple, Set, Any

# 配置日志 - 只写入文件，不在控制台显示
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("etf_rotation.log", encoding='utf-8')
    ]
)

# 连接DuckDB数据库
conn = duckdb.connect('data.db')

# 创建ETF数据表（添加主键约束）
conn.execute("""
CREATE TABLE IF NOT EXISTS etf_data_15min (
    code VARCHAR,
    trade_datetime DATETIME,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume FLOAT,
    amount FLOAT,
    PRIMARY KEY (code, trade_datetime)
)
""")

# 定义要获取的ETF代码列表
etf_codes = [
    ('518880', 1),
    ('513300', 1),
    ('159915', 0),  # T+1
    ('588000', 1),  # T+1
    ('161128', 0),
    ('513520', 1),
    ('159870', 0),  # T+1
    ('159980', 0),
    ('511090', 1)
]

# T+1基金列表
T1_ETFS = {'159915', '588000', '159870'}

# 策略参数配置
INITIAL_CAPITAL = 50000  # 初始资金
LOOKBACK_PERIOD = 30  # 回溯周期改为30个15分钟K线
TOP_N = 2  # 选择前N名


# 获取ETF数据并存入数据库
def fetch_etf_data_15min():
    api = TdxHq_API()

    with api.connect("60.12.136.250", 7709):
        for code, market in etf_codes:
            print(f"正在获取 {code} 的15分钟数据...")
            data = []

            # 计算需要获取的页数 (获取最近3个月的数据，约60个交易日 * 16个15分钟K线 = 960条)
            pages = 30  # 30*800=24000条，足够3个月数据

            for i in range(pages):
                # 使用1表示15分钟K线
                chunk = api.get_security_bars(1, market, code, i * 800, 800)
                if chunk:
                    data.extend(chunk)

            # 转换为DataFrame
            df = api.to_df(data)

            # 处理数据
            df['code'] = code
            df['trade_datetime'] = pd.to_datetime(df['datetime'])
            df.rename(columns={
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'vol': 'volume',
                'amount': 'amount'
            }, inplace=True)

            # 选择需要的列
            df = df[
                ['code', 'trade_datetime', 'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'amount']]

            # 存入数据库 - 先删除可能存在的重复数据
            for _, row in df.iterrows():
                conn.execute(f"""
                    DELETE FROM etf_data_15min 
                    WHERE code = '{row['code']}' AND trade_datetime = '{row['trade_datetime']}'
                """)

            # 插入新数据
            conn.register('temp_df', df)
            conn.execute("INSERT INTO etf_data_15min SELECT * FROM temp_df")

    print("所有ETF 15分钟数据获取完成并已存入数据库")


# 判断是否为交易时间
def is_trading_time(dt):
    """判断给定时间是否为交易时间"""
    time = dt.time()
    # 上午交易时间: 9:30-11:30
    am_start = pd.Timestamp('09:30:00').time()
    am_end = pd.Timestamp('11:30:00').time()
    # 下午交易时间: 13:00-15:00
    pm_start = pd.Timestamp('13:00:00').time()
    pm_end = pd.Timestamp('15:00:00').time()

    return ((am_start <= time <= am_end) or (pm_start <= time <= pm_end)) and dt.weekday() < 5


# 获取下一个交易时间
def get_next_trading_time(current_time):
    """获取下一个交易时间（开盘时间）"""
    next_time = current_time + timedelta(minutes=15)

    # 如果下一个时间不是交易时间，则找到下一个交易日的开盘时间
    while not is_trading_time(next_time):
        next_time += timedelta(minutes=15)
        # 如果是收盘时间，跳到下一个交易日
        if next_time.time() > pd.Timestamp('15:00:00').time():
            next_time = next_time + timedelta(days=1)
            next_time = next_time.replace(hour=9, minute=30, second=0)

    return next_time


# 计算涨幅排名
def calculate_rankings(data: pd.DataFrame, current_time: datetime) -> List[Tuple[str, float]]:
    """计算当前时间点的ETF涨幅排名"""
    rankings = []

    # 获取所有ETF代码
    all_codes = data['code'].unique()

    for code in all_codes:
        # 获取该ETF的数据
        etf_data = data[data['code'] == code].sort_values('trade_datetime')

        # 找到当前时间点之前的数据
        past_data = etf_data[etf_data['trade_datetime'] <= current_time]

        # 确保有足够的数据计算涨幅
        if len(past_data) >= LOOKBACK_PERIOD + 1:
            # 获取最近LOOKBACK_PERIOD+1条数据
            recent_data = past_data.tail(LOOKBACK_PERIOD + 1)

            # 计算涨幅（从第1条到第LOOKBACK_PERIOD+1条）
            start_price = recent_data.iloc[0]['close_price']
            end_price = recent_data.iloc[-1]['close_price']

            # 避免除以零的情况
            if start_price > 0:
                returns = (end_price - start_price) / start_price * 100
                rankings.append((code, returns))
            else:
                # 如果起始价格为0，设为负无穷大，排名最后
                rankings.append((code, -float('inf')))

    # 按涨幅降序排序
    rankings.sort(key=lambda x: x[1], reverse=True)

    return rankings


# 执行ETF轮动策略回测
def run_etf_rotation_strategy():
    """执行ETF轮动策略回测"""
    # 从数据库读取所有ETF数据
    query = "SELECT * FROM etf_data_15min ORDER BY trade_datetime, code"
    all_data = conn.execute(query).fetchdf()

    # 获取所有时间点
    all_times = sorted(all_data['trade_datetime'].unique())

    # 初始化投资组合
    cash = INITIAL_CAPITAL  # 现金
    # 持仓结构: {code: (shares, buy_date, locked)}
    # locked: True表示该持仓被锁定（T+1 ETF当天买入），不能卖出
    holdings: Dict[str, Tuple[float, datetime, bool]] = {}
    portfolio_value = INITIAL_CAPITAL  # 投资组合总价值
    trade_log = []

    # 遍历每个时间点（从第LOOKBACK_PERIOD+1个时间点开始）
    for i in range(LOOKBACK_PERIOD, len(all_times) - 1):
        current_time = all_times[i]
        next_time = all_times[i + 1]
        current_date = current_time.date()

        # 更新持仓锁状态：检查T+1 ETF是否应该解锁（买入日期不是当天）
        codes_to_unlock = []
        for code, (shares, buy_date, locked) in holdings.items():
            if code in T1_ETFS and locked and buy_date.date() < current_date:
                codes_to_unlock.append(code)

        for code in codes_to_unlock:
            shares, buy_date, _ = holdings[code]
            holdings[code] = (shares, buy_date, False)
            logging.info(f"解锁持仓: {code}, 时间: {current_time}")

        # 计算当前时间点的排名
        rankings = calculate_rankings(all_data, current_time)

        # 获取前TOP_N名
        top_etfs = [code for code, _ in rankings[:TOP_N]]

        # 检查是否需要调仓
        need_rebalance = False

        # 获取当前未锁定的持仓代码
        unlocked_holdings = [code for code, (_, _, locked) in holdings.items() if not locked]

        # 如果没有持仓，需要调仓
        if not unlocked_holdings:
            need_rebalance = True
        else:
            # 检查未锁定的持仓是否都在前TOP_N名中
            for code in unlocked_holdings:
                if code not in top_etfs:
                    need_rebalance = True
                    break

        # 如果需要调仓且是交易时间
        if need_rebalance and is_trading_time(current_time):
            # 计算当前持仓价值
            holdings_value = 0
            etfs_to_sell = []  # 需要卖出的ETF（未锁定的且不在前TOP_N名中的）

            # 计算当前持仓价值并确定哪些需要卖出
            for code, (shares, buy_date, locked) in holdings.items():
                # 获取当前价格
                current_data = all_data[(all_data['code'] == code) &
                                        (all_data['trade_datetime'] == current_time)]
                if not current_data.empty:
                    current_price = current_data.iloc[0]['open_price']
                    # 确保价格有效
                    if not np.isnan(current_price) and current_price > 0:
                        holdings_value += shares * current_price

                        # 检查是否需要卖出：未锁定且不在前TOP_N名中
                        if not locked and code not in top_etfs:
                            etfs_to_sell.append((code, shares))

            # 计算总资产
            portfolio_value = cash + holdings_value
            profit_loss = portfolio_value - INITIAL_CAPITAL
            profit_loss_pct = (profit_loss / INITIAL_CAPITAL) * 100 if INITIAL_CAPITAL > 0 else 0

            # 执行卖出操作（未锁定且不在前TOP_N名中的ETF）
            for code, shares in etfs_to_sell:
                # 获取当前价格
                current_data = all_data[(all_data['code'] == code) &
                                        (all_data['trade_datetime'] == current_time)]
                if not current_data.empty:
                    current_price = current_data.iloc[0]['open_price']
                    # 确保价格有效
                    if not np.isnan(current_price) and current_price > 0:
                        # 卖出持仓
                        cash += shares * current_price

                        # 从持仓中移除
                        if code in holdings:
                            del holdings[code]

                        # 记录卖出操作
                        log_message = f"卖出: {code} {shares:.2f}股 @ {current_price:.4f}, 时间: {current_time}"
                        logging.info(log_message)

            # 等权重买入前TOP_N名ETF
            if cash > 0 and top_etfs:
                cash_per_etf = cash / len(top_etfs)
                for code in top_etfs:
                    # 获取当前价格
                    current_data = all_data[(all_data['code'] == code) &
                                            (all_data['trade_datetime'] == current_time)]
                    if not current_data.empty:
                        current_price = current_data.iloc[0]['open_price']
                        # 确保价格有效
                        if not np.isnan(current_price) and current_price > 0:
                            shares = cash_per_etf / current_price

                            # 确定是否锁定：T+1 ETF且当天买入
                            locked = code in T1_ETFS and current_time.date() == current_date

                            # 检查是否已经持有该ETF
                            if code in holdings:
                                # 如果已经持有，增加持仓
                                existing_shares, existing_date, existing_locked = holdings[code]

                                # 如果原有持仓已解锁，但新买入的是T+1 ETF，需要锁定
                                if not existing_locked and locked:
                                    holdings[code] = (existing_shares + shares, current_time, True)
                                else:
                                    # 保持原有锁定状态
                                    holdings[code] = (existing_shares + shares, existing_date, existing_locked)
                            else:
                                # 如果没有持有，创建新持仓
                                holdings[code] = (shares, current_time, locked)

                            # 更新现金
                            cash -= cash_per_etf

                            # 记录买入操作
                            log_message = f"买入: {code} {shares:.2f}股 @ {current_price:.4f}, 时间: {current_time}, 锁定: {locked}"
                            logging.info(log_message)

            # 记录调仓信息到日志文件
            log_message = (
                f"调仓时间: {current_time}, "
                f"排名: {[(code, f'{returns:.2f}%') for code, returns in rankings]}, "
                f"选择ETF: {top_etfs}, "
                f"持仓情况: {[(code, f'{shares:.2f}股', buy_date, f'锁定: {locked}') for code, (shares, buy_date, locked) in holdings.items()]}, "
                f"现金: {cash:.2f}元, "
                f"持仓价值: {holdings_value:.2f}元, "
                f"资产总值: {portfolio_value:.2f}元, "
                f"盈亏: {profit_loss:.2f}元 ({profit_loss_pct:.2f}%)"
            )
            logging.info(log_message)
            trade_log.append(log_message)

        # 更新投资组合价值（用于下一个时间点）
        holdings_value = 0
        for code, (shares, _, _) in holdings.items():
            # 获取下一个时间点的收盘价
            next_data = all_data[(all_data['code'] == code) &
                                 (all_data['trade_datetime'] == next_time)]
            if not next_data.empty:
                next_close = next_data.iloc[0]['close_price']
                # 确保价格有效
                if not np.isnan(next_close) and next_close > 0:
                    holdings_value += shares * next_close

        portfolio_value = cash + holdings_value

    # 计算最终盈亏
    final_profit = portfolio_value - INITIAL_CAPITAL
    final_profit_pct = (final_profit / INITIAL_CAPITAL) * 100 if INITIAL_CAPITAL > 0 else 0

    # 记录最终结果到日志文件
    final_log = (
        f"策略回测完成, "
        f"初始资金: {INITIAL_CAPITAL:.2f}元, "
        f"最终资产: {portfolio_value:.2f}元, "
        f"总盈亏: {final_profit:.2f}元 ({final_profit_pct:.2f}%)"
    )
    logging.info(final_log)

    return trade_log, portfolio_value, final_profit_pct


# 主函数
def main():
    # 获取数据
    print("开始获取ETF数据...")
    fetch_etf_data_15min()

    # 运行策略
    print("开始执行ETF轮动策略回测...")
    trade_log, final_value, final_return = run_etf_rotation_strategy()

    # 打印最终结果到控制台
    print(f"\n策略回测完成!")
    print(f"初始资金: {INITIAL_CAPITAL:.2f}元")
    print(f"最终资产: {final_value:.2f}元")
    print(f"总收益率: {final_return:.2f}%")
    print(f"详细交易记录已保存到 etf_rotation.log 文件中")


if __name__ == "__main__":
    main()