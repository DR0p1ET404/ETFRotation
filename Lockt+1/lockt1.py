import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytdx.hq import TdxHq_API
import logging
from typing import List, Dict, Tuple, Set, Any
import time
import os

# 配置日志 - 同时写入文件和控制台
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(message)s',
	handlers=[
		logging.FileHandler("etf_rotation.log", encoding='utf-8'),
		logging.StreamHandler()
	]
)

# 连接DuckDB数据库
conn = duckdb.connect('data.db')

# 定义要获取的ETF代码列表
etf_codes = [
	('518880', 1),
	('159985', 0),
	('159980', 0),
	('159981', 0),
	('511380', 1),
	('511090', 1),
	('588000', 1),
	('159920', 0),
	('513090', 1),
	('513120', 1),
	('513130', 1),
	('159915', 0),
	('159941', 0),
	('513500', 1),
	('159509', 0),
	('513520', 1),
	('513030', 1),
	('511360', 1),
	('511880', 1)
]

# T+1基金列表
T1_ETFS = {'159915', '588000'}

# 策略参数配置
INITIAL_CAPITAL = 50000  # 初始资金
LOOKBACK_PERIOD = 30  # 回溯周期改为30个15分钟K线
TOP_N = 2  # 选择前N名


# 为每个ETF创建单独的表
def create_etf_tables():
	"""为每个ETF创建单独的表"""
	for code, _ in etf_codes:
		# 创建表，使用trade_datetime作为主键
		conn.execute(f"""
        CREATE TABLE IF NOT EXISTS etf_{code} (
            trade_datetime DATETIME PRIMARY KEY,
            open_price FLOAT,
            close_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            volume FLOAT,
            amount FLOAT
        )
        """)
	logging.info("所有ETF表创建完成")


# 获取ETF数据并存入数据库
def fetch_etf_data_15min():
	"""获取ETF数据并存入对应的表中"""
	api = TdxHq_API()
	create_etf_tables()  # 确保表已创建

	with api.connect("60.12.136.250", 7709):
		for code, market in etf_codes:
			logging.info(f"正在获取 {code} 的15分钟数据...")
			data = []

			# 计算需要获取的页数 (获取最近3个月的数据，约60个交易日 * 16个15分钟K线 = 960条)
			pages = 30  # 30*800=24000条，足够3个月数据

			for i in range(pages):
				# 使用1表示15分钟K线
				chunk = api.get_security_bars(1, market, code, i * 800, 800)
				if chunk:
					data.extend(chunk)
				time.sleep(0.1)  # 添加延迟避免请求过于频繁

			# 转换为DataFrame
			df = api.to_df(data)

			if df.empty:
				logging.warning(f"代码 {code} 没有获取到数据，跳过")
				continue

			# 处理数据
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
			df = df[['trade_datetime', 'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'amount']]

			# 确保价格数据有效
			df = df[(df['open_price'] > 0) & (df['close_price'] > 0) &
					(df['high_price'] > 0) & (df['low_price'] > 0)]

			if df.empty:
				logging.warning(f"代码 {code} 有效数据为空，跳过")
				continue

			# 存入对应的ETF表 - 先删除可能存在的重复数据
			for _, row in df.iterrows():
				conn.execute(f"""
                    DELETE FROM etf_{code} 
                    WHERE trade_datetime = '{row['trade_datetime']}'
                """)

			# 插入新数据
			conn.register('temp_df', df)
			conn.execute(f"INSERT INTO etf_{code} SELECT * FROM temp_df")
			logging.info(f"ETF {code} 数据已存入数据库，共 {len(df)} 条记录")

	logging.info("所有ETF 15分钟数据获取完成并已存入数据库")


# 判断是否为交易时间
def is_trading_time(dt):
	"""判断给定时间是否为交易时间"""
	time_val = dt.time()
	# 上午交易时间: 9:30-11:30
	am_start = pd.Timestamp('09:30:00').time()
	am_end = pd.Timestamp('11:30:00').time()
	# 下午交易时间: 13:00-15:00
	pm_start = pd.Timestamp('13:00:00').time()
	pm_end = pd.Timestamp('15:00:00').time()

	return ((am_start <= time_val <= am_end) or (pm_start <= time_val <= pm_end)) and dt.weekday() < 5


# 从数据库获取所有ETF数据
def get_all_etf_data():
	"""从所有ETF表中获取数据并合并"""
	all_data = pd.DataFrame()

	for code, _ in etf_codes:
		# 查询该ETF的数据
		query = f"SELECT * FROM etf_{code} ORDER BY trade_datetime"
		df = conn.execute(query).fetchdf()

		if not df.empty:
			# 添加ETF代码列
			df['code'] = code
			# 合并数据
			all_data = pd.concat([all_data, df], ignore_index=True)

	logging.info(f"从数据库获取了 {len(all_data)} 条ETF数据记录")
	return all_data


# 计算涨幅排名
def calculate_rankings(all_data: pd.DataFrame, current_time: datetime) -> List[Tuple[str, float]]:
	"""计算当前时间点的ETF涨幅排名"""
	rankings = []

	# 获取所有ETF代码
	all_codes = all_data['code'].unique()

	for code in all_codes:
		# 获取该ETF的数据
		etf_data = all_data[all_data['code'] == code].sort_values('trade_datetime')

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
			if start_price > 0 and not np.isnan(start_price) and not np.isnan(end_price):
				returns = (end_price - start_price) / start_price * 100
				rankings.append((code, returns))
			else:
				# 如果起始价格为0或NaN，设为负无穷大，排名最后
				rankings.append((code, -float('inf')))
		else:
			# 数据不足，设为负无穷大
			rankings.append((code, -float('inf')))

	# 按涨幅降序排序
	rankings.sort(key=lambda x: x[1], reverse=True)

	return rankings


# 执行ETF轮动策略回测
def run_etf_rotation_strategy():
	"""执行ETF轮动策略回测"""
	# 从数据库读取所有ETF数据
	all_data = get_all_etf_data()

	# 检查数据是否足够
	if len(all_data) == 0:
		logging.error("没有获取到数据，请先运行fetch_etf_data_15min()获取数据")
		return [], INITIAL_CAPITAL, 0

	# 获取所有时间点
	all_times = sorted(all_data['trade_datetime'].unique())
	logging.info(f"共有 {len(all_times)} 个时间点")

	# 确保有足够的时间点进行回测
	if len(all_times) <= LOOKBACK_PERIOD:
		logging.error(f"数据不足，需要至少{LOOKBACK_PERIOD + 1}个时间点，当前只有{len(all_times)}个")
		return [], INITIAL_CAPITAL, 0

	# 预先为每个ETF准备数据，提高效率
	etf_data_dict = {}
	for code in all_data['code'].unique():
		etf_data = all_data[all_data['code'] == code].sort_values('trade_datetime')
		etf_data_dict[code] = etf_data

	# 初始化投资组合
	cash = INITIAL_CAPITAL  # 现金
	# 持仓结构: {code: (shares, buy_date, locked)}
	# locked: True表示该持仓被锁定（T+1 ETF当天买入），不能卖出
	holdings: Dict[str, Tuple[float, datetime, bool]] = {}
	portfolio_value = INITIAL_CAPITAL  # 投资组合总价值
	trade_log = []

	# 记录初始状态
	initial_log = f"初始资金: {INITIAL_CAPITAL:.2f}元"
	logging.info(initial_log)
	trade_log.append(initial_log)

	# 添加进度计数器
	total_steps = len(all_times) - LOOKBACK_PERIOD - 1
	processed_steps = 0
	last_progress_log = 0

	# 预先计算所有时间点的排名
	rankings_by_time = {}
	logging.info("开始预先计算所有时间点的排名...")
	for i in range(LOOKBACK_PERIOD, len(all_times) - 1):
		current_time = all_times[i]
		rankings = calculate_rankings(all_data, current_time)
		rankings_by_time[current_time] = rankings

		# 记录进度
		if i % 100 == 0:
			progress = (i - LOOKBACK_PERIOD) / (len(all_times) - LOOKBACK_PERIOD - 1) * 100
			logging.info(f"排名计算进度: {progress:.1f}%")

	logging.info("排名计算完成")

	# 遍历每个时间点（从第LOOKBACK_PERIOD+1个时间点开始）
	for i in range(LOOKBACK_PERIOD, len(all_times) - 1):
		current_time = all_times[i]
		next_time = all_times[i + 1]
		current_date = current_time.date()

		# 每处理100个时间点记录一次进度
		processed_steps += 1
		if processed_steps - last_progress_log >= 100 or processed_steps == total_steps:
			progress = processed_steps / total_steps * 100
			logging.info(f"回测进度: {processed_steps}/{total_steps} ({progress:.1f}%)")
			last_progress_log = processed_steps

		# 记录当前时间点
		logging.debug(f"处理时间点: {current_time}")

		# 更新持仓锁状态：检查T+1 ETF是否应该解锁（买入日期不是当天）
		codes_to_unlock = []
		for code, (shares, buy_date, locked) in holdings.items():
			if code in T1_ETFS and locked and buy_date.date() < current_date:
				codes_to_unlock.append(code)

		for code in codes_to_unlock:
			shares, buy_date, _ = holdings[code]
			holdings[code] = (shares, buy_date, False)
			logging.info(f"解锁持仓: {code}, 时间: {current_time}")

		# 获取预先计算好的排名
		rankings = rankings_by_time[current_time]
		logging.debug(f"排名结果: {rankings}")

		# 获取前TOP_N名
		top_etfs = [code for code, _ in rankings[:TOP_N]]
		logging.debug(f"前{TOP_N}名ETF: {top_etfs}")

		# 检查是否需要调仓
		need_rebalance = False

		# 获取当前持仓代码
		current_holdings = list(holdings.keys())
		logging.debug(f"当前持仓: {current_holdings}")

		# 如果持仓与排名前两名不一致，需要调仓
		if set(current_holdings) != set(top_etfs):
			need_rebalance = True
			logging.debug(f"需要调仓: 持仓 {current_holdings} 与排名前两名 {top_etfs} 不一致")

		# 计算当前持仓价值
		holdings_value = 0
		etfs_to_sell = []  # 需要卖出的ETF（未锁定的且不在前TOP_N名中的）

		# 计算当前持仓价值并确定哪些可以卖出
		for code, (shares, buy_date, locked) in holdings.items():
			# 获取当前价格
			current_data = etf_data_dict[code][etf_data_dict[code]['trade_datetime'] == current_time]
			if not current_data.empty:
				current_price = current_data.iloc[0]['close_price']
				# 确保价格有效
				if not np.isnan(current_price) and current_price > 0:
					holdings_value += shares * current_price

					# 检查是否需要卖出：不在前TOP_N名中且可以卖出（未锁定）
					if code not in top_etfs and not locked:
						etfs_to_sell.append((code, shares))
						logging.debug(f"需要卖出: {code}, 锁定状态: {locked}")

		# 计算总资产
		portfolio_value = cash + holdings_value
		profit_loss = portfolio_value - INITIAL_CAPITAL
		profit_loss_pct = (profit_loss / INITIAL_CAPITAL) * 100 if INITIAL_CAPITAL > 0 else 0

		# 如果需要调仓且是交易时间
		if need_rebalance and is_trading_time(current_time) and (len(etfs_to_sell) > 0 or len(holdings) == 0):
			logging.info(f"执行调仓操作，需要卖出的ETF: {etfs_to_sell}")

			# 执行卖出操作（可以卖出的且不在前TOP_N名中的ETF）
			for code, shares in etfs_to_sell:
				# 获取当前价格
				current_data = etf_data_dict[code][etf_data_dict[code]['trade_datetime'] == current_time]
				if not current_data.empty:
					current_price = current_data.iloc[0]['close_price']
					# 确保价格有效
					if not np.isnan(current_price) and current_price > 0:
						# 卖出持仓
						sell_value = shares * current_price
						cash += sell_value

						# 从持仓中移除
						if code in holdings:
							del holdings[code]

						# 记录卖出操作
						log_message = f"卖出: {code} {shares:.2f}股 @ {current_price:.4f}, 时间: {current_time}, 获得: {sell_value:.2f}元"
						logging.info(log_message)
						trade_log.append(log_message)

			# 确定需要买入的ETF（新的前两名中尚未持有的）
			etfs_to_buy = [code for code in top_etfs if code not in holdings]
			logging.debug(f"需要买入的ETF: {etfs_to_buy}")

			# 如果有需要买入的ETF且有现金
			if cash > 0 and etfs_to_buy:
				# 计算可用于购买的资金
				available_cash = cash * 0.999

				# 等权重分配资金
				cash_per_etf = available_cash / len(etfs_to_buy) if etfs_to_buy else 0

				for code in etfs_to_buy:
					# 获取当前价格
					current_data = etf_data_dict[code][etf_data_dict[code]['trade_datetime'] == current_time]
					if not current_data.empty:
						current_price = current_data.iloc[0]['close_price']
						# 确保价格有效
						if not np.isnan(current_price) and current_price > 0:
							shares = cash_per_etf / current_price

							# 确定是否锁定：T+1 ETF且当天买入
							locked = code in T1_ETFS and current_time.date() == current_date

							# 检查是否已经持有该ETF
							if code in holdings:
								# 如果已经持有，增加持仓
								existing_shares, existing_date, existing_locked = holdings[code]
								holdings[code] = (existing_shares + shares, existing_date, existing_locked or locked)
							else:
								# 如果没有持有，创建新持仓
								holdings[code] = (shares, current_time, locked)

							# 更新现金
							cash -= cash_per_etf

							# 记录买入操作
							log_message = f"买入: {code} {shares:.2f}股 @ {current_price:.4f}, 时间: {current_time}, 花费: {cash_per_etf:.2f}元, 锁定: {locked}"
							logging.info(log_message)
							trade_log.append(log_message)

			# 记录调仓信息到日志文件
			log_message = (
				f"调仓时间: {current_time}, "
				f"排名: {[(code, f'{returns:.2f}%') for code, returns in rankings]}, "
				f"选择ETF: {top_etfs}, "
				f"持仓情况: {[(code, f'{shares:.2f}股', f'锁定: {locked}') for code, (shares, _, locked) in holdings.items()]}, "
				f"现金: {cash:.2f}元, "
				f"持仓价值: {holdings_value:.2f}元, "
				f"资产总值: {portfolio_value:.2f}元, "
				f"盈亏: {profit_loss:.2f}元 ({profit_loss_pct:.2f}%)"
			)
			logging.info(log_message)
			trade_log.append(log_message)
		else:
			# 记录不需要调仓的情况
			log_message = (
				f"时间点: {current_time}, "
				f"不需要调仓, "
				f"排名: {[(code, f'{returns:.2f}%') for code, returns in rankings]}, "
				f"持仓: {[(code, f'{shares:.2f}股', f'锁定: {locked}') for code, (shares, _, locked) in holdings.items()]}, "
				f"现金: {cash:.2f}元, "
				f"持仓价值: {holdings_value:.2f}元, "
				f"资产总值: {portfolio_value:.2f}元, "
				f"盈亏: {profit_loss:.2f}元 ({profit_loss_pct:.2f}%)"
			)
			logging.debug(log_message)

		# 更新投资组合价值（用于下一个时间点）
		holdings_value = 0
		for code, (shares, _, _) in holdings.items():
			# 获取下一个时间点的收盘价
			next_data = etf_data_dict[code][etf_data_dict[code]['trade_datetime'] == next_time]
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
	trade_log.append(final_log)

	return trade_log, portfolio_value, final_profit_pct


# 获取特定ETF在特定时间的数据
def get_etf_data_at_time(code, time_point):
	"""获取特定ETF在特定时间的数据"""
	query = f"SELECT * FROM etf_{code} WHERE trade_datetime = '{time_point}'"
	result = conn.execute(query).fetchdf()
	return result


# 获取特定ETF在时间范围内的数据
def get_etf_data_in_range(code, start_time, end_time):
	"""获取特定ETF在时间范围内的数据"""
	query = f"SELECT * FROM etf_{code} WHERE trade_datetime >= '{start_time}' AND trade_datetime <= '{end_time}' ORDER BY trade_datetime"
	result = conn.execute(query).fetchdf()
	return result


# 主函数
def main():
	# 检查数据库文件是否存在，如果存在则删除
	# if os.path.exists('data.db'):
	# 	os.remove('data.db')
	# 	logging.info("删除旧的数据库文件")

	# 重新连接数据库
	global conn
	conn = duckdb.connect('data.db')

	# 获取数据
	logging.info("开始获取ETF数据...")
	# fetch_etf_data_15min()

	# 运行策略
	logging.info("开始执行ETF轮动策略回测...")
	trade_log, final_value, final_return = run_etf_rotation_strategy()

	# 打印最终结果到控制台
	logging.info(f"\n策略回测完成!")
	logging.info(f"初始资金: {INITIAL_CAPITAL:.2f}元")
	logging.info(f"最终资产: {final_value:.2f}元")
	logging.info(f"总收益率: {final_return:.2f}%")
	logging.info(f"详细交易记录已保存到 etf_rotation.log 文件中")


if __name__ == "__main__":
	# 设置日志级别为INFO
	logging.getLogger().setLevel(logging.INFO)
	main()