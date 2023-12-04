import datetime
import sys
import time
import typing

import requests
import re
import bs4
import pandas as pd
from loguru import logger
from pydantic import BaseModel
from tqdm import tqdm

sys.path.append('./')
from financialdata_v.router import Router

def crawler_team(
    team: str,
) -> pd.DataFrame:
    url = f"https://www.mlb.com/{team}/stats/"
    print(url)
    res = requests.get(url)
    soup = bs4.BeautifulSoup(res.text, "html5lib")
    # 全部球員的紀錄
    players_record = []
    try:
        # soup 變成tbody
        soup = soup.find("tbody", class_= 'notranslate')
        # records
        records = soup.find_all('tr')
        for record in records:
            # name 相關的資料
            name_relate = record.find_all("span",class_= "full-3fV3c9pF")
            name = name_relate[0].text + " " + name_relate[1].text
            position = record.find("div", class_ = "position-28TbwVOg").text

            # 個別球員的記錄
            play_record = [name, position]

            bat_records = record.find_all("td", scope = "row")
            for bat_record in bat_records:
                header = bat_record["headers"][0]
                if re.search(r'tb-\d+-header-col[2|3|4|5|6|7|8|9|10|11|12|13|15|16]', header):
                    play_record.append(bat_record.text)
                else:
                    pass

            players_record.append(play_record)
            df = pd.DataFrame(players_record, columns = ['Name', 'POS', 'Team', 'G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'SO', 'SB', 'CS', 'AVG', 'OBP', 'SLG', 'OPS'])
    except:
        df = "Error"
    return df

def clear_data(
    df: pd.DataFrame
) -> pd.DataFrame:
    for x in ['G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'SO', 'SB', 'CS']:
        df[x] = df[x].apply(lambda x: int(x))
    for x in ['AVG', 'OBP', 'SLG', 'OPS']:
        df[x] = df[x].apply(lambda x: float(x))

    df = df[df['AB'] != 0]
    df = df.drop(columns = ['AVG', 'OPS'])

    df = df.rename(columns = {
                '2B':'Double',
                '3B':'Third'
                })

    return df

class BaseballRecord(BaseModel):
    Name: str
    POS: str
    Team: str
    G: int
    AB: int
    R: int
    H: int
    Double: int
    Third: int
    HR: int
    RBI: int
    BB: int
    SO: int
    SB: int
    CS: int
    OBP: float
    SLG: float

def check_schema(
        df: pd.DataFrame,
) -> pd.DataFrame:
    """檢查資料型態, 確保每次要上傳資料庫前, 型態正確"""
    df_dict = df.to_dict("records")
    df_schema = [
        BaseballRecord(**dd).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df
    
    
def main(
    team: typing.List[str]
) -> pd.DataFrame:
    all_team = pd.DataFrame({})
    for t in team:
        team_record = crawler_team(t)
        team_record = clear_data(team_record)
        team_record = check_schema(team_record)
        all_team = all_team.append(team_record)
    all_team.reset_index(inplace= True, drop= True)

    db_router = Router()

    try:
        all_team.to_sql(
            name = "all_team_data",
            con = db_router.mysql_mlb_data_conn,
            if_exists = "replace",
            index = False,
            chunksize = 1000
        )
    except Exception as e:
        logger.info(e)
    return all_team
    
    
    

if __name__ == "__main__":
    team = sys.argv[1:]
    a = main(team)
    print(a)