from time import sleep
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, String, SmallInteger, DateTime
from datetime import datetime
from sqlalchemy.orm import mapper, Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from typing import Optional
from sqlalchemy.orm import DeclarativeBase, declarative_base
from datetime import datetime
from sqlalchemy import String
from datetime import datetime, timedelta
import akshare as ak

# 先读取数据库当中的search_stocks表，获取当前需要存储信息的股票
engine = create_engine('mysql+pymysql://root:MJ57N01o6iTFxt2An5DH@containers-us-west-92.railway.app:7780/railway')
metadata = MetaData()

class Base(DeclarativeBase):
    pass

def get_table(name):
    DynamicBase = declarative_base(class_registry=dict())
    # 一次性创建一个表
    class StockInfo(DynamicBase):
        __tablename__ = name
        __table_args__ = {'extend_existing': True}
        id: Mapped[int] = mapped_column(primary_key=True)
        time: Mapped[Optional[str]] = mapped_column(String(30))
        open: Mapped[Optional[float]] = mapped_column()
        close: Mapped[Optional[float]] = mapped_column()
        high: Mapped[Optional[float]] = mapped_column()
        low: Mapped[Optional[float]] = mapped_column()
        attitude: Mapped[Optional[float]] = mapped_column()
        attitude_account: Mapped[Optional[float]] = mapped_column()
        scalar: Mapped[Optional[float]] = mapped_column()
        account: Mapped[Optional[float]] = mapped_column()
        amplitude: Mapped[Optional[float]] = mapped_column()
        rate: Mapped[Optional[float]] = mapped_column()

        def __repr__(self) -> str:
            return f"id: {self.id}, 时间: {self.time}, 开盘价: {self.open}, 收盘价: {self.close},\
                    最高: {self.high}, 最低: {self.low}, 涨跌幅: {self.attitude}, 涨跌额: {self.attitude_account}, 成交量: {self.scalar},\
                    成交额: {self.account}, 振幅: {self.amplitude}, 换手率: {self.rate}"
    return StockInfo

# class StockInfo(Base):
#     __tablename__ = 'test'
#     __table_args__ = {'extend_existing': True}
#     id: Mapped[int] = mapped_column(primary_key=True)
#     time: Mapped[Optional[str]] = mapped_column(String(30))
#     open: Mapped[Optional[float]] = mapped_column()
#     close: Mapped[Optional[float]] = mapped_column()
#     high: Mapped[Optional[float]] = mapped_column()
#     low: Mapped[Optional[float]] = mapped_column()
#     attitude: Mapped[Optional[float]] = mapped_column()
#     attitude_account: Mapped[Optional[float]] = mapped_column()
#     scalar: Mapped[Optional[float]] = mapped_column()
#     account: Mapped[Optional[float]] = mapped_column()
#     amplitude: Mapped[Optional[float]] = mapped_column()
#     rate: Mapped[Optional[float]] = mapped_column()

#     def __repr__(self) -> str:
#         return f"id: {self.id}, 时间: {self.time}, 开盘价: {self.open}, 收盘价: {self.close},\
#                 最高: {self.high}, 最低: {self.low}, 涨跌幅: {self.attitude}, 涨跌额: {self.attitude_account}, 成交量: {self.scalar},\
#                 成交额: {self.account}, 振幅: {self.amplitude}, 换手率: {self.rate}"

while True:
    today = datetime.today()
    # 第一层if 确保在股票的交易时间，即周一到周五的上午9.30-11.30， 下午的13.00-15.00
    if (0 <= today.weekday() <= 4 
        and ((today.hour == 9 and today.minute >= 30) or (today.hour == 10) or (today.hour == 11 and today.minute <= 30)
        or (today.hour >= 13 and today.hour <= 15))):
            # 第二层if 确保在30分钟的时候，因为插入的都是30分钟的数据
            if today.minute == 0 or today.minute == 30:
                with Session(engine) as session:
                    search_stocks = session.execute(text("select * from search_stocks3")).all()
                    tables = session.execute(text("show tables")).all()
                    for stock in search_stocks:
                        # 因为'show tables'返回的结果是元组的形势，所以也要生成元组的形式与之匹配
                        stock_tuple = (stock[1],)
                        # print(stock_tuple)
                        # print(stock_tuple not in tables)
                        if stock_tuple not in tables:
                            # print(f"{stock[1]} is not in this db")
                            # new_stock = Table(
                                
                            # )
                            
                            NewStock = get_table(stock[1])
                            # print(new_stock.__table__)
                            # 创建一个单张的表
                            NewStock.__table__.create(bind=engine)
                            
                            now = datetime.today()
                            pre = now - timedelta(days=200)
                            pre_pre = pre - timedelta(days=200)
                            now = now.strftime('%Y-%m-%d %H:%M:%S')
                            pre = pre.strftime('%Y-%m-%d %H:%M:%S')
                            # 获取当前股票30分钟的交易数据
                            current_stock_list = ak.stock_zh_a_hist_min_em(symbol=stock[1], start_date=pre, 
                                                                    end_date=now, period='30', adjust='')
                            for i in range(len(current_stock_list)):
                                tmp = current_stock_list.iloc[i, :]
                                session.add(NewStock(time=tmp['时间'], open=tmp['开盘'], close=tmp['收盘'], high=tmp['最高'],
                                                        low=tmp['最低'], attitude=tmp['涨跌幅'], attitude_account=tmp['涨跌额'],
                                                        scalar=tmp['成交量'], account=tmp['成交额'], amplitude=tmp['振幅'], rate=tmp['换手率']))
                            session.commit()
                        else:
                            # print(f"{stock[1]}已经在数据库中了")
                            CurrentStockTable = get_table(stock[1])
                            stock_realtime_df = ak.stock_zh_a_spot_em()
                            realtime_df = stock_realtime_df[stock_realtime_df['代码'] == stock[1]]
                            now = datetime.today()
                            tmp = CurrentStockTable(time=now, close=realtime_df.iloc[0]['最新价'], attitude=realtime_df.iloc[0]["涨跌幅"], 
                                                attitude_account=realtime_df.iloc[0]["涨跌额"], scalar=realtime_df.iloc[0]["成交量"], 
                                                account=realtime_df.iloc[0]["成交额"], amplitude=realtime_df.iloc[0]["振幅"], 
                                                rate=realtime_df.iloc[0]["换手率"])
                            session.add(tmp)
                            session.commit()
    # 判断完一次要间隔一分钟
    sleep(1)