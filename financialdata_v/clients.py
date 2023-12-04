from sqlalchemy import (
    create_engine,
    engine,
)


def get_mysql_financialdata_conn() -> engine.base.Connection:
    """
    user: root
    password: test
    host: localhost
    port: 3306
    database: financialdata
    如果有實體 IP，以上設定可以自行更改
    """
    address = "mysql+pymysql://root:test@localhost:3306/financialdata"
    engine = create_engine(address)
    connect = engine.connect()
    return connect

def get_mysql_mlb_data_conn() -> engine.base.Connection:
    """
    user: root
    password: test
    host: localhost
    port: 3306
    database: mlb_data
    如果有實體 IP，以上設定可以自行更改
    """
    address = "mysql+pymysql://root:test@localhost:3306/mlb_data"
    engine = create_engine(address)
    connect = engine.connect()
    return connect