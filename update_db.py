from backend_app import  GetHistoricalData, check_exist_table_cup
from get_tm_securities import take_new_tinkoff_list
import os
from sqlalchemy import create_engine
import asyncio


def update_history_data_bd():
    delete_bd()


    engine = create_engine(r'sqlite:///db/test1.db')
    connection  = engine.connect()
    hist = GetHistoricalData()
    asyncio.run(hist.get_new_securities(connection))
    take_new_tinkoff_list()
    asyncio.run(hist.get_candles_history(connection))
    connection.close()

def delete_bd():
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'db/test1.db')
    if os.path.exists(path):
        os.remove(path)



if __name__ == "__main__":
    update_history_data_bd()