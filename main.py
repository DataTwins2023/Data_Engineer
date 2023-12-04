import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine, engine
import typing

def get_mysql_financialdata_conn() -> engine.base.Connection:
    address = "mysql+pymysql://root:test@127.0.0.1:3306/financialdata"
    engine = create_engine(address)
    connect = engine.connect()
    return connect

def get_mysql_mlb_data_conn() -> engine.base.Connection:
    address = "mysql+pymysql://root:test@127.0.0.1:3306/mlb_data"
    engine = create_engine(address)
    connect = engine.connect()
    return connect


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/taiwan_stock_price")
def taiwan_stock_price(
    stock_id: str = "",
    start_date: str = "",
    end_date: str = "",
):
    sql = f"""
    select * from TaiwanStockPrice
    where StockID = '{stock_id}'
    and Date>= '{start_date}'
    and Date<= '{end_date}'
    """
    mysql_conn = get_mysql_financialdata_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}

@app.get("/mlb")
def mlb_data(
    team: str = "",
):
    sql = f"""
    select * from all_team_data
    where Team = '{team}'
    """
    mysql_conn = get_mysql_mlb_data_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}

@app.get("/mlb_hr")
def mlb_data_hr(
    HR: int = "",
):
    sql = f"""
    select * from all_team_data
    where HR >= '{HR}'
    ORDER BY HR
    """
    mysql_conn = get_mysql_mlb_data_conn()
    data_df = pd.read_sql(sql, con=mysql_conn)
    data_dict = data_df.to_dict("records")
    return {"data": data_dict}