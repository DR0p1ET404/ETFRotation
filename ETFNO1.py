import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytdx.hq import TdxHq_API

# 连接DuckDB数据库
conn = duckdb.connect('data.db')

# 删除旧表（如果存在）
conn.execute("DROP TABLE IF EXISTS etf_data")

# 创建新的ETF数据表（包含trade_time列）
conn.execute("""
CREATE TABLE etf_data (
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
	('512400', 1)
]


# 获取ETF数据并存入数据库
def fetch_etf_data():
	api = TdxHq_API()

	with api.connect("60.12.136.250", 7709):
		for code, market in etf_codes:
			print(f"正在获取 {code} 的数据...")
			data = []

			# 计算需要获取的页数 (20000条数据，每页800条)
			pages = 25  # 20000/800 = 25页

			for i in range(pages):
				chunk = api.get_security_bars(2, market, code, i * 800, 800)
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
			df = df[
				['code', 'trade_date', 'trade_time', 'open_price', 'close_price', 'high_price', 'low_price', 'volume',
				 'amount']]

			# 删除重复数据
			df = df.drop_duplicates(subset=['code', 'trade_date', 'trade_time'])

			# 存入数据库
			for _, row in df.iterrows():
				try:
					conn.execute(f"""
                        INSERT INTO etf_data 
                        (code, trade_date, trade_time, open_price, close_price, high_price, low_price, volume, amount)
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

	print("所有ETF数据获取完成并已存入数据库")


# 计算移动平均涨幅
def calculate_ma_returns(code, current_date, current_time, etf_df, periods=[5, 10, 20]):
	"""
	计算指定ETF在指定日期和时间的多个周期的移动平均涨幅
	"""
	# 获取该ETF过去20个30分钟K线的数据
	etf_data = etf_df[(etf_df['code'] == code) &
					  ((etf_df['trade_date'] < current_date) |
					   ((etf_df['trade_date'] == current_date) & (etf_df['trade_time'] <= current_time)))]

	etf_data = etf_data.sort_values(['trade_date', 'trade_time']).tail(max(periods))

	if len(etf_data) < max(periods):
		return None  # 数据不足

	# 计算每个K线的涨幅
	returns = etf_data['close_price'].pct_change().dropna().tail(max(periods))

	if len(returns) < max(periods):
		return None

	# 计算各周期移动平均涨幅
	ma_returns = {}
	for period in periods:
		ma_returns[period] = returns.tail(period).mean()

	return ma_returns


# 计算每个ETF的30日涨幅
def calculate_30day_returns(code, current_date, etf_df):
	"""
	计算指定ETF在指定日期的30日涨幅
	"""
	# 获取该ETF最近30天的日线数据（取每日收盘价）
	daily_data = etf_df[etf_df['code'] == code].copy()
	daily_data = daily_data[daily_data['trade_date'] <= current_date]

	# 按日期分组，取每日最后一条记录（收盘价）
	daily_close = daily_data.groupby('trade_date').last().reset_index()
	daily_close = daily_close.sort_values('trade_date').tail(30)

	if len(daily_close) < 30:
		return None

	# 计算涨幅
	start_price = daily_close.iloc[0]['close_price']
	end_price = daily_close.iloc[-1]['close_price']

	# 避免除零错误
	if start_price <= 0:
		return None

	thirty_day_return = (end_price - start_price) / start_price * 100
	return thirty_day_return


# 策略回测函数
def backtest_etf_rotation(etf_df, initial_capital=1000000):
	# 获取所有ETF代码
	codes = [c[0] for c in etf_codes]

	# 创建回测结果DataFrame
	results = []

	# 初始化投资组合
	portfolio = {
		'cash': initial_capital,
		'holdings': {},  # {code: {'shares': shares, 'cost_price': cost_price}}
		'value': initial_capital
	}

	# 创建调仓记录文件
	with open('etf_rotation_log.txt', 'w', encoding='utf-8') as log_file:
		log_file.write("ETF轮动策略调仓记录\n")
		log_file.write("=" * 80 + "\n\n")
		log_file.write("策略模式: 始终持仓，不空仓\n\n")

	# 获取所有交易日
	trade_dates = sorted(etf_df['trade_date'].unique())

	# 循环每个交易日
	for i, current_date in enumerate(trade_dates):
		# 跳过前60个交易日，因为我们至少需要60天数据来计算均线
		if i < 60:
			continue

		# 获取当日所有30分钟K线的时间点
		time_points = sorted(etf_df[etf_df['trade_date'] == current_date]['trade_time'].unique())

		# 计算每个ETF的30日涨幅
		etf_performance = []
		for code in codes:
			thirty_day_return = calculate_30day_returns(code, current_date, etf_df)
			if thirty_day_return is not None:
				etf_performance.append({
					'code': code,
					'thirty_day_return': thirty_day_return
				})

		# 按30日涨幅排序
		etf_performance.sort(key=lambda x: x['thirty_day_return'], reverse=True)

		# 获取top1和top2
		top1 = etf_performance[0] if len(etf_performance) > 0 else None
		top2 = etf_performance[1] if len(etf_performance) > 1 else None

		# 记录30日涨幅排名
		with open('etf_rotation_log.txt', 'a', encoding='utf-8') as log_file:
			log_file.write(f"日期: {current_date}\n")
			log_file.write("-" * 80 + "\n")
			log_file.write("30日涨幅排名:\n")
			for rank, etf in enumerate(etf_performance, 1):
				log_file.write(f"  {rank}. {etf['code']}: {etf['thirty_day_return']:.2f}%\n")
			if top1 and top2:
				log_file.write(f"目标持仓: {top1['code']} (排名第一), {top2['code']} (排名第二)\n")
			log_file.write("-" * 80 + "\n\n")

		# 循环每个30分钟K线
		for j, current_time in enumerate(time_points):
			# 计算当前K线结束时的投资组合价值
			portfolio_value = portfolio['cash']
			position_values = {}

			for code, position in portfolio['holdings'].items():
				# 获取当前价格
				current_price_data = etf_df[(etf_df['code'] == code) &
											(etf_df['trade_date'] == current_date) &
											(etf_df['trade_time'] == current_time)]
				if len(current_price_data) > 0:
					current_price = current_price_data['close_price'].values[0]
					position_value = position['shares'] * current_price
					portfolio_value += position_value
					position_values[code] = {
						'value': position_value,
						'cost_value': position['shares'] * position['cost_price'],
						'profit': position_value - (position['shares'] * position['cost_price']),
						'profit_pct': (position_value / (position['shares'] * position['cost_price']) - 1) * 100,
						'current_price': current_price
					}

			# 记录每个K线结束时的结果
			results.append({
				'date': current_date,
				'time': current_time,
				'portfolio_value': portfolio_value,
				'cash': portfolio['cash'],
				'holdings': {code: pos['shares'] for code, pos in portfolio['holdings'].items()}
			})

			# 记录每个K线结束时的盈亏情况和排名
			with open('etf_rotation_log.txt', 'a', encoding='utf-8') as log_file:
				log_file.write(f"日期: {current_date} 时间: {current_time}\n")
				log_file.write(f"投资组合总价值: {portfolio_value:.2f}\n")
				log_file.write(f"现金: {portfolio['cash']:.2f}\n")

				# 记录当前持仓
				if portfolio['holdings']:
					log_file.write("当前持仓:\n")
					for code, position in portfolio['holdings'].items():
						if code in position_values:
							log_file.write(f"  {code}: {position['shares']} 股, "
										   f"成本价: {position['cost_price']:.4f}, "
										   f"当前价: {position_values[code]['current_price']:.4f}, "
										   f"盈亏: {position_values[code]['profit']:.2f} "
										   f"({position_values[code]['profit_pct']:.2f}%)\n")
				else:
					log_file.write("当前持仓: 无\n")

				# 记录当前排名
				log_file.write("当前30日涨幅排名:\n")
				for rank, etf in enumerate(etf_performance[:5], 1):  # 只显示前5名
					log_file.write(f"  {rank}. {etf['code']}: {etf['thirty_day_return']:.2f}%\n")

				log_file.write("-" * 80 + "\n")

			# 如果不是最后一个K线，执行调仓
			if j < len(time_points) - 1:
				next_time = time_points[j + 1]

				with open('etf_rotation_log.txt', 'a', encoding='utf-8') as log_file:
					log_file.write(f"日期: {current_date} 时间: {current_time} -> 调仓操作\n")

					# 检查当前持仓是否需要调整
					current_codes = set(portfolio['holdings'].keys())
					target_codes = set()
					if top1:
						target_codes.add(top1['code'])
					if top2:
						target_codes.add(top2['code'])

					# 记录目标持仓
					log_file.write(f"目标持仓: {', '.join(target_codes) if target_codes else '无'}\n")

					# 如果当前持仓与目标持仓一致，则不需要调仓
					if current_codes == target_codes and len(target_codes) > 0:
						log_file.write("持仓与目标一致，无需调仓\n")
						log_file.write("-" * 80 + "\n\n")
						continue

					# 记录切换情况
					if current_codes:
						log_file.write(f"当前持仓: {', '.join(current_codes)}\n")
						if target_codes:
							to_sell = current_codes - target_codes
							to_buy = target_codes - current_codes

							if to_sell:
								log_file.write(f"需要卖出的ETF: {', '.join(to_sell)}\n")
							if to_buy:
								log_file.write(f"需要买入的ETF: {', '.join(to_buy)}\n")

							if not to_sell and not to_buy:
								log_file.write("持仓与目标一致，无需调仓\n")
							else:
								log_file.write("执行调仓操作:\n")
						else:
							log_file.write("无目标持仓，清空所有持仓\n")
					else:
						log_file.write("当前无持仓\n")
						if target_codes:
							log_file.write(f"需要买入的ETF: {', '.join(target_codes)}\n")
							log_file.write("执行建仓操作:\n")

					# 清空所有现有持仓（使用下一条K线的开盘价）
					for code in list(portfolio['holdings'].keys()):
						open_price_data = etf_df[(etf_df['code'] == code) &
												 (etf_df['trade_date'] == current_date) &
												 (etf_df['trade_time'] == next_time)]
						if len(open_price_data) > 0:
							open_price = open_price_data['open_price'].values[0]
							sale_value = portfolio['holdings'][code]['shares'] * open_price
							portfolio['cash'] += sale_value
							log_file.write(
								f"  卖出 {code}: {portfolio['holdings'][code]['shares']} 股 @ {open_price:.4f} "
								f"(价值: {sale_value:.2f})\n")
							del portfolio['holdings'][code]

					# 执行新计划：买入排名前两的ETF
					if top1 and top2:
						# 获取开盘价
						price1_data = etf_df[(etf_df['code'] == top1['code']) &
											 (etf_df['trade_date'] == current_date) &
											 (etf_df['trade_time'] == next_time)]
						price2_data = etf_df[(etf_df['code'] == top2['code']) &
											 (etf_df['trade_date'] == current_date) &
											 (etf_df['trade_time'] == next_time)]

						if len(price1_data) > 0 and len(price2_data) > 0:
							price1 = price1_data['open_price'].values[0]
							price2 = price2_data['open_price'].values[0]

							# 计算购买份额
							total_value = portfolio['cash']
							shares1 = int((total_value * 0.5) / price1) if price1 > 0 else 0
							shares2 = int((total_value * 0.5) / price2) if price2 > 0 else 0

							# 更新持仓
							if shares1 > 0:
								portfolio['holdings'][top1['code']] = {
									'shares': shares1,
									'cost_price': price1
								}
								cost1 = shares1 * price1
								log_file.write(
									f"  买入 {top1['code']}: {shares1} 股 @ {price1:.4f} (成本: {cost1:.2f})\n")

							if shares2 > 0:
								portfolio['holdings'][top2['code']] = {
									'shares': shares2,
									'cost_price': price2
								}
								cost2 = shares2 * price2
								log_file.write(
									f"  买入 {top2['code']}: {shares2} 股 @ {price2:.4f} (成本: {cost2:.2f})\n")

							# 剩余现金
							portfolio['cash'] = total_value - (shares1 * price1 + shares2 * price2)
					else:
						log_file.write("无法执行买入操作: 数据不足\n")

					log_file.write(f"调仓后现金余额: {portfolio['cash']:.2f}\n")
					log_file.write("=" * 80 + "\n\n")

		# 如果资金归零，提前结束回测
		if portfolio_value <= 0:
			print(f"资金归零，提前结束回测。日期：{current_date}")
			break

	return pd.DataFrame(results)


# 执行数据获取
fetch_etf_data()

# 从数据库读取数据
etf_df = conn.execute("SELECT * FROM etf_data ORDER BY code, trade_date, trade_time").fetchdf()

# 检查数据完整性
print("检查数据完整性...")
for code in [c[0] for c in etf_codes]:
	code_data = etf_df[etf_df['code'] == code]
	print(
		f"{code}: {len(code_data)} 条记录, 日期范围: {code_data['trade_date'].min()} 到 {code_data['trade_date'].max()}")

# 运行回测
print("\n开始回测...")
results_df = backtest_etf_rotation(etf_df)

# 计算回测指标
if len(results_df) > 0 and results_df.iloc[-1]['portfolio_value'] > 0:
	initial_value = results_df.iloc[0]['portfolio_value']
	final_value = results_df.iloc[-1]['portfolio_value']
	total_return = (final_value - initial_value) / initial_value * 100

	# 计算年化收益率
	days = (results_df.iloc[-1]['date'] - results_df.iloc[0]['date']).days
	if days > 0:
		annualized_return = (1 + total_return / 100) ** (365 / days) - 1
	else:
		annualized_return = 0

	# 计算最大回撤
	max_drawdown = 0
	peak = results_df['portfolio_value'].iloc[0]
	for value in results_df['portfolio_value']:
		if value > peak:
			peak = value
		drawdown = (peak - value) / peak
		if drawdown > max_drawdown:
			max_drawdown = drawdown

	# 输出回测结果
	print("\n回测结果:")
	print(f"初始资金: {initial_value:,.2f}")
	print(f"最终资金: {final_value:,.2f}")
	print(f"总收益率: {total_return:.2f}%")
	print(f"年化收益率: {annualized_return * 100:.2f}%")
	print(f"最大回撤: {max_drawdown * 100:.2f}%")
	print(f"回测期间: {results_df.iloc[0]['date']} 到 {results_df.iloc[-1]['date']}")
	print(f"策略模式: 始终持仓，不空仓")
	print(f"详细调仓记录已保存到 etf_rotation_log.txt")

	# 计算夏普比率
	daily_returns = results_df['portfolio_value'].pct_change().dropna()
	if len(daily_returns) > 0 and daily_returns.std() > 0:
		sharpe_ratio = np.sqrt(252) * daily_returns.mean() / daily_returns.std()
		print(f"夏普比率: {sharpe_ratio:.4f}")
	else:
		print("夏普比率: 无法计算 (标准差为0)")

	# 计算胜率
	positive_days = len(daily_returns[daily_returns > 0])
	total_days = len(daily_returns)
	win_rate = positive_days / total_days * 100 if total_days > 0 else 0
	print(f"胜率: {win_rate:.2f}% ({positive_days}/{total_days} 天)")

	# 计算月收益率
	monthly_returns = []
	monthly_dates = []
	current_month = None
	monthly_start_value = None

	for _, row in results_df.iterrows():
		month = row['date'].strftime('%Y-%m')
		if month != current_month:
			if current_month is not None and monthly_start_value is not None and monthly_start_value > 0:
				monthly_return = (row['portfolio_value'] - monthly_start_value) / monthly_start_value * 100
				monthly_returns.append(monthly_return)
				monthly_dates.append(current_month)

			current_month = month
			monthly_start_value = row['portfolio_value']

	# 添加最后一个月
	if current_month is not None and monthly_start_value is not None and monthly_start_value > 0:
		monthly_return = (results_df.iloc[-1]['portfolio_value'] - monthly_start_value) / monthly_start_value * 100
		monthly_returns.append(monthly_return)
		monthly_dates.append(current_month)

	# 输出月收益率
	if monthly_returns:
		print("\n月收益率:")
		for date, ret in zip(monthly_dates, monthly_returns):
			print(f"{date}: {ret:.2f}%")

		# 计算平均月收益率
		avg_monthly_return = sum(monthly_returns) / len(monthly_returns)
		print(f"平均月收益率: {avg_monthly_return:.2f}%")

		# 输出最佳和最差月份
		best_month = max(monthly_returns)
		worst_month = min(monthly_returns)
		best_month_index = monthly_returns.index(best_month)
		worst_month_index = monthly_returns.index(worst_month)

		print(f"最佳月份: {monthly_dates[best_month_index]} ({best_month:.2f}%)")
		print(f"最差月份: {monthly_dates[worst_month_index]} ({worst_month:.2f}%)")
	else:
		print("\n无法计算月收益率")
else:
	print("回测结果为空或最终资金为0，可能因为数据不足或策略失效")

# 关闭数据库连接
conn.close()