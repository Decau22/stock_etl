import os
import pandas as pd
from datetime import datetime, timedelta
from tiingo import TiingoClient
import hashlib
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, ForeignKey
from sqlalchemy.sql import select, delete
from sqlalchemy import Integer, BigInteger, Float, String, Numeric, DATE
from sqlalchemy.orm import sessionmaker


def hashFields(inputDataFrame, fields=None):
    if not fields:
        fields = inputDataFrame.columns

    df = inputDataFrame[fields].astype('str')
    key = df.apply(''.join, axis=1)
    id = key.map(lambda x: hashlib.sha1(x.encode('utf-8')).hexdigest())
    return id


def connect2Postgres(uname, pwd, host, port, db):
    URL = "postgresql://%s:%s@%s:%s/%s" %(uname, pwd, host, port, db)
    engine = create_engine(URL, isolation_level="AUTOCOMMIT")

    return engine


def Upsert(engine, etl_df, dest_table, key):
    tablename = dest_table.name
    schemaname = dest_table.schema
    # Delete previous records in destination table
    Session = sessionmaker(bind=engine)
    session = Session()
    nrows_prev = session.query(dest_table).filter(dest_table.c[key].in_(etl_df[key])).count()
    session.query(dest_table).filter(dest_table.c[key].in_(etl_df[key])).\
        delete(synchronize_session=False)
    session.commit()

    # Insert latest records to destination table
    try:
        etl_df.to_sql(name=tablename, con=engine,
                      schema=schemaname, if_exists="append", index=False)
        nrows_all = etl_df.shape[0]
        nrows_new = nrows_all - nrows_prev
        print("%s rows affected in table %s, including %s previous rows and %s new rows " \
                % (nrows_all, dest_table, nrows_prev, nrows_new))
        return None
    except Exception as e:
        print(e)
        print("There was an error. Here is the data that was attempted to be inserted:")
        print(etl_df[key])
        return 'Error'

def main():
  DAYS_BACK = 7
  TIINGO_KEY = os.environ.get('TIINGO_API_KEY')
  pg_user = os.environ.get('POSTGRE_UNAME')
  pg_pwd = os.environ.get('POSTGRE_PWD')
  pg_host = os.environ.get('POSTGRE_HOST')
  pg_port = os.environ.get('POSTGRE_PORT')
  pg_database = 'stock'
#   pg_database = 'stock_dev'

  client = TiingoClient()

  start_date = (datetime.today() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
  # start_date = '2020-01-01'
  end_date = datetime.today().strftime('%Y-%m-%d')

  TICKER_LIST = ['GOOGL', 'AAPL', 'AMD', 'XLK' , 'NIO', 'NVDA', 'EXPE', 'BABA']
  METRIC_LIST = ['adjClose',
                 'adjHigh',
                 'adjLow',
                 'adjOpen',
                 'adjVolume',
                 'close',
                 'divCash',
                 'high',
                 'low',
                 'open',
                 'splitFactor',
                 'volume'
                ]
  
  ticker_history = {}
  for metric in METRIC_LIST:
    ticker_history[metric] = client.get_dataframe(TICKER_LIST,
                                        frequency='daily',
                                        metric_name=metric,
                                        startDate=start_date,
                                        endDate=end_date)
    
    ticker_history[metric] = ticker_history[metric].stack().reset_index()
    ticker_history[metric].rename(columns={'level_1':'ticker', 0:metric}, errors="raise", inplace=True)

  df = ticker_history[METRIC_LIST[0]]
  for n in range(1, len(METRIC_LIST)):
    df = pd.merge(df, ticker_history[METRIC_LIST[n]], how="outer", on=['date', 'ticker'])

  df.rename(columns={'date':'full_date'}, inplace=True)
  df['pk_hash'] = hashFields(df[['full_date', 'ticker']])

  engine = connect2Postgres(pg_user, pg_pwd, pg_host, pg_port, pg_database)
  metadata = MetaData()

  DEST_SCHEMA = 'stockdata'
  DEST_TABLE = 'daily_position'
  position_tbl = Table(DEST_TABLE, metadata,
                      Column('pk_hash', String, nullable=False, primary_key=True),
                      Column('full_date', DATE),
                      Column('ticker', String),
                      Column('adjClose', Numeric),
                      Column('adjHigh', Numeric),
                      Column('adjLow', Numeric),
                      Column('adjOpen', Numeric),
                      Column('adjVolume', Integer),
                      Column('close', Numeric),
                      Column('divCash', Numeric),
                      Column('high', Numeric),
                      Column('low', Numeric),
                      Column('open', Numeric),
                      Column('splitFactor', Numeric),
                      Column('volume', Integer),
                      schema=DEST_SCHEMA
                      )

  metadata.create_all(engine)

  Upsert(engine, df, position_tbl, 'pk_hash')


if __name__ == '__main__':
    main()