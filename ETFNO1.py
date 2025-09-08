import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytdx.hq import TdxHq_API
import logging

# 设置日志 - 文件日志记录所有信息，控制台只记录关键信息
file_handler = logging.FileHandler("etf_backtest.log", encoding='utf-8')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # 控制台只显示警告和错误

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(message)s',
	handlers=[file_handler, console_handler]
)
logger = logging.getLogger()

# 连接DuckDB数据库
conn = duckdb.connect('data.db')

# 删除旧表（如果存在）
conn.execute("DROP TABLE IF EXISTS etf_data_30min")

# 创建新的ETF数据表（包含trade_time列）
conn.execute("""
CREATE TABLE etf_data_30min (
    code VARCHAR,
    trade_date DATE,
    trade_time TIME,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume FLOAT,
    amount FLOAT,
    PRIMARY KEY (code, trade_date, trade_time)
)
""")

# 定义要获取的ETF代码列表
etf_codes = [
	('518880', 1),
	('513300', 1),
	('159915', 0),
	('588000', 1),
	('161128', 0),
	('513520', 1),
	('159870', 0),
	('512400', 1),
	('511090', 1)
]


# 获取ETF 30分钟K线数据并存入数据库
def fetch_etf_30min_data():
	api = TdxHq_API()

	with api.connect("60.12.136.250", 7709):
		for code, market in etf_codes:
			print(f"正在获取 {code} 的30分钟K线数据...")
			data = []

			# 获取30分钟K线数据 (周期参数为5)
			pages = 25  # 获取大约25页数据，约20000条

			for i in range(pages):
				chunk = api.get_security_bars(5, market, code, i * 800, 800)
				if chunk:
					data.extend(chunk)

			# 转换为DataFrame
			df = api.to_df(data)

			# 处理数据
			df['code'] = code
			df['trade_date'] = pd.to_datetime(df['datetime']).dt.date
			df['trade_time'] = pd.to_datetime(df['datetime']).dt.time
			df.rename(columns={
				'open': 'open_price',
				'close': 'close_price',
				'high': 'high_price',
				'low': 'low_price',
				'vol': 'volume',
				'amount': 'amount'
			}, inplace=True)

			# 选择需要的列
			df = df[['code', 'trade_date', 'trade_time', 'open_price', 'close_price',
					 'high_price', 'low_price', 'volume', 'amount']]

			# 删除重复数据
			df = df.drop_duplicates(subset=['code', 'trade_date', 'trade_time'])

			# 存入数据库
			for _, row in df.iterrows():
				try:
					conn.execute(f"""
                        INSERT INTO etf_data_30min 
                        (code, trade_date, trade_time, open_price, close_price, 
                         high_price, low_price, volume, amount)
                        VALUES (
                            '{row['code']}', 
                            '{row['trade_date']}', 
                            '{row['trade_time']}', 
                            {row['open_price']}, 
                            {row['close_price']}, 
                            {row['high_price']}, 
                            {row['low_price']}, 
                            {row['volume']}, 
                            {row['amount']}
                        )
                    """)
				except Exception as e:
					print(f"插入数据时出错: {e}")
					continue

	print("所有ETF 30分钟K线数据获取完成并已存入数据库")


# 回测函数
def backtest_strategy():
	# 从数据库读取所有ETF的30分钟K线数据
	df = conn.execute("""
        SELECT code, trade_date, trade_time, open_price, close_price
        FROM etf_data_30min 
        ORDER BY trade_date, trade_time
    """).fetchdf()

	# 将日期和时间合并为datetime
	df['datetime'] = pd.to_datetime(df['trade_date'].astype(str) + ' ' + df['trade_time'].astype(str))

	# 获取所有唯一的时间点
	all_times = sorted(df['datetime'].unique())

	# 初始化变量
	initial_cash = 50000
	cash = initial_cash
	holdings = {}  # 当前持仓 {code: shares}
	portfolio_value = []  # 记录每个时间点的投资组合价值
	trade_log = []  # 交易日志

	# 记录每个ETF的30周期涨幅历史
	returns_history = {}
	for code in [c[0] for c in etf_codes]:
		returns_history[code] = []

	# 遍历每个时间点
	for i, current_time in enumerate(all_times):
		# 只从第30个时间点开始计算（因为有30根K线）
		if i < 30:
			continue

		logger.info(f"\n=== 时间点: {current_time} ===")

		# 计算每个ETF的30周期涨幅
		returns = {}
		for code in [c[0] for c in etf_codes]:
			# 获取该ETF最近30根K线的数据
			code_data = df[df['code'] == code].sort_values('datetime')
			code_data = code_data[code_data['datetime'] <= current_time].tail(30)

			if len(code_data) >= 30:
				start_price = code_data.iloc[0]['close_price']
				end_price = code_data.iloc[-1]['close_price']
				returns[code] = (end_price - start_price) / start_price * 100
				returns_history[code].append((current_time, returns[code]))
			else:
				returns[code] = -100  # 如果数据不足，设为极低值

		# 按涨幅排序
		sorted_returns = sorted(returns.items(), key=lambda x: x[1], reverse=True)

		# 记录排名情况
		rank_info = ", ".join([f"{code}: {ret:.2f}%" for code, ret in sorted_returns])
		logger.info(f"30周期涨幅排名: {rank_info}")

		# 选择前2名
		top_2 = [item[0] for item in sorted_returns[:2]]
		logger.info(f"选择前2名: {top_2}")

		# 获取当前时间点的价格
		current_prices = {}
		for code in top_2:
			price_df = df[(df['code'] == code) & (df['datetime'] == current_time)]
			if not price_df.empty:
				current_prices[code] = price_df['close_price'].values[0]

		# 如果有持仓，计算当前持仓价值
		current_value = cash
		for code, shares in holdings.items():
			price_df = df[(df['code'] == code) & (df['datetime'] == current_time)]
			if not price_df.empty:
				price = price_df['close_price'].values[0]
				current_value += shares * price

		# 记录当前投资组合价值
		portfolio_value.append((current_time, current_value))

		# 检查是否需要调仓（当前持仓与排名前2不一致）
		need_rebalance = False
		current_holdings = set(holdings.keys())
		target_holdings = set(top_2)

		if current_holdings != target_holdings:
			need_rebalance = True
			logger.info(f"持仓变化: {current_holdings} -> {target_holdings}")

		if need_rebalance:
			# 卖出所有现有持仓
			for code, shares in list(holdings.items()):
				price_df = df[(df['code'] == code) & (df['datetime'] == current_time)]
				if not price_df.empty:
					sell_price = price_df['close_price'].values[0]
					cash += shares * sell_price
					logger.info(f"卖出 {code}: {shares}股 @ {sell_price:.4f}")
					trade_log.append({
						'time': current_time,
						'action': '卖出',
						'code': code,
						'shares': shares,
						'price': sell_price,
						'value': shares * sell_price
					})

			holdings = {}

			# 买入新的ETF
			if len(top_2) > 0 and current_value > 0:
				# 计算每只ETF的分配金额
				allocation_per_etf = current_value / len(top_2)

				for code in top_2:
					if code in current_prices:
						buy_price = current_prices[code]
						shares = allocation_per_etf / buy_price
						holdings[code] = shares
						cash -= shares * buy_price
						logger.info(f"买入 {code}: {shares:.2f}股 @ {buy_price:.4f}")
						trade_log.append({
							'time': current_time,
							'action': '买入',
							'code': code,
							'shares': shares,
							'price': buy_price,
							'value': shares * buy_price
						})

		# 记录当前状态
		logger.info(f"现金: {cash:.2f}, 持仓: {holdings}")
		total_holdings_value = 0
		for code, shares in holdings.items():
			price_df = df[(df['code'] == code) & (df['datetime'] == current_time)]
			if not price_df.empty:
				price = price_df['close_price'].values[0]
				total_holdings_value += shares * price
		logger.info(f"总资产: {cash + total_holdings_value:.2f}")

	# 计算最终结果
	final_value = cash
	for code, shares in holdings.items():
		# 使用最后时间点的价格计算持仓价值
		last_time = all_times[-1]
		price_df = df[(df['code'] == code) & (df['datetime'] == last_time)]
		if not price_df.empty:
			price = price_df['close_price'].values[0]
			final_value += shares * price

	total_return = (final_value - initial_cash) / initial_cash * 100

	# 使用warning级别在控制台显示最终结果
	logger.warning("\n=== 回测结果 ===")
	logger.warning(f"初始资金: {initial_cash:.2f}")
	logger.warning(f"最终资金: {final_value:.2f}")
	logger.warning(f"总收益率: {total_return:.2f}%")

	# 计算最大回撤
	max_drawdown = 0
	peak = initial_cash
	for time, value in portfolio_value:
		if value > peak:
			peak = value
		drawdown = (peak - value) / peak * 100
		if drawdown > max_drawdown:
			max_drawdown = drawdown

	logger.warning(f"最大回撤: {max_drawdown:.2f}%")

	# 打印交易统计
	logger.warning(f"总交易次数: {len(trade_log)}")

	return trade_log, portfolio_value, returns_history


# 主函数
def main():
	# 获取30分钟K线数据
	fetch_etf_30min_data()

	# 运行回测
	trade_log, portfolio_value, returns_history = backtest_strategy()

	# 关闭数据库连接
	conn.close()


if __name__ == "__main__":
	main()