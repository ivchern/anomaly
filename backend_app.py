import asyncio
from time import sleep
import aiomoex
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.io.sql import read_sql, table_exists
from sqlalchemy import create_engine, MetaData
import datetime
import math
from get_tm_securities import take_new_tinkoff_list
from tinkof import check_secid, get_dict_secid_figi
from get_tinkoff_cup import get_percentage_list
from bot_message import send_message
import time
import os


def get_list_securities(connection): 
    '''Получить cписок акции из DB
    '''
    securities = read_sql(sql='securities', con = connection, index_col=False)
    securities_list = list(securities['SECID'])
    return securities_list


def get_diference_price(df): 
    '''Возвращает разницу в цене
    '''   
    diference_list = list()
    for _, row in df.iterrows():
        differnce = row['open'] - row['close']
        differnce = math.sqrt(pow(differnce, 2))
        diference_list.append(differnce)
        if len(df) == 2: 
            differnce_precent = (row['open'] - row['close'])/row['open']
            diference_list.append(round(differnce_precent,3))
            diference_list.append(row['close'])
            break
    return diference_list


def take_time(past=True, month = False):
    '''Возвращает время для парсинга
        :param past: False возвращает строку времени в данный момент
        :params month: Возвращает значение 3 месяца назада. По умолчанию дневной
    '''
    now_time = datetime.datetime.today()
    if past == True:   
        if month == True:
            time_1min_ago = now_time - datetime.timedelta(days = 91)
        else:
            time_1min_ago = now_time - datetime.timedelta(days = 6, minutes=15)
        return datetime.datetime.strftime(time_1min_ago, '%Y-%m-%dT%H:%M')
    else:
        return datetime.datetime.strftime(now_time, '%Y-%m-%dT%H:%M')


class GetHistoricalData(object):
    async def get_new_securities(self, connection): 
        '''Получить cписок доступных акций на MOEX
        '''
        async with aiomoex.ISSClientSession(): 
            securities = await aiomoex.get_board_securities(columns=['SECID', 'LOTSIZE', 'SHORTNAME'])
            df = pd.DataFrame(securities) 
            df.to_sql(name = 'securities', con = connection, if_exists='replace', index=False, method='multi') 
            # take_new_tinkoff_list()
            print('Список бумаг получен')


    async def get_candles_history(self, connection):
        """Получить данные по акциям за период: 'volume', 'close'            
        """
        async with aiomoex.ISSClientSession():
            securities_list = get_list_securities(connection)
            for security in securities_list:
                data = await aiomoex.get_board_candles(security, 1, take_time(month=True), take_time(past=False, month=True), columns=['volume', 'open', 'close'])
                df = pd.DataFrame(data) 
                if not df.empty:
                    diference_list = get_diference_price(df)
                    df.drop('close', axis=1, inplace=True)
                    df.drop('open', axis=1, inplace=True)
                    df['difference'] = pd.Series(diference_list, index=df.index)
                    df.to_sql(name = f'{security}', con = connection, if_exists='replace') # add method='multi'


async def anomaly_volume(data, volume, secid, price_pricent, price_now, figi, shortname, time_min_ago, connection):   #todo
    '''Выявляет является ли свеча аномальной по отношению к объему
    data - исторические данные, 
    volume - объем текущей свечи
    price_pricent - изменение цены в процентах
    price_now - текущая цена
    '''
    date_time = time_min_ago.strftime("%Y-%m-%d %H:%M")          
    data_volume = list(data['volume'])
    data_volume.append(volume)
    data_volume = set(data_volume) 
    data_volume = list(data_volume) 
    sorted_data = sorted(data_volume)
    indx = sorted_data.index(volume)
    if(indx/len(sorted_data) > 0.2): 
        cap = get_cup(time_min_ago, figi, connection)              
        buy = cap['buy']
        sell = cap['sell']
        msg = (str(f' #{secid} {shortname}\n <strong>$$4 Аномальное объем</strong>\n Изменение цены: {price_pricent}%\n Цена: {price_now}Р' +\
            f'\n Покупка: {buy}% Продажа: {sell}%\n Объем: {volume}\n Время: {date_time}')) 
        await send_message(msg)
        await asyncio.sleep(0.5)  
        print(msg)



async def anomaly_close(data, price_change, secid, price_pricent, price_now, figi, volume, shortname, time_min_ago, connection):  #todo
    '''Выявляет является ли свеча аномальной по отношению к цене
    data - исторические данные, 
    price_pricent - изменение цены
    price_change - изменение цены свечки,
    secid - тикет акции,
    price_pricent - изменение цены свечки в процентах
    price_now - текущая цена
    ''' 
    date_time = time_min_ago.strftime("%m-%d-%Y %H:%M")              #todo
    data_close = list(data['difference'])
    new_data_close = list()
    for data in data_close: 
        data = str(data)
        new_data_close.append(data)
    new_data_close.append(f'{price_change}')
    new_data_close.sort()
    index_num = new_data_close.index(f'{price_change}')
    if ((index_num+1)/len(new_data_close)) > 0.85: 
        cap = get_cup(time_min_ago, figi, connection)             #todo
        buy = cap['buy']              
        sell = cap['sell']
        msg = (str(f'#{secid} {shortname}\n <strong>$$4 Аномальное изменение цены</strong> \n  Изменение цены: {price_pricent}%\n Цена: {price_now}Р')+\
            (f'\n Покупка: {buy}% Продажа: {sell}%\n Объем: {volume}\n Время: {date_time}')) 
        await send_message(msg)
        await asyncio.sleep(0.5)  
        print(msg)
 

def download_cup(connection): 
    '''Получение стакана
    '''
    dict_cup = dict()
    time_now = datetime.datetime.today() 
    time_now_str = time_now.strftime("%H%M")
    list_figi = get_dict_secid_figi()
    for figi, _ in list_figi.items():
        cap = get_percentage_list(figi)
        buy = cap['buy']
        sell = cap['sell']
        dict_cup.update({figi: [buy, sell]})
        time.sleep(0.02)
    cup_df = pd.DataFrame.from_dict(dict_cup, orient='index', columns=['buy', 'sell'])
    cup_df.to_sql(name = f'cup{time_now_str}', con = connection, if_exists='replace', index_label='figi')
    

def delete_cup_two_min_ago(time_min_ago, engine):
    '''Удаление неактульных данных по стакану
    '''
    name_db = name_from_time_db(time_min_ago)
    engine.execute(f"DROP TABLE IF EXISTS {name_db};")


def get_cup(time_min_ago, figi, connection):
    '''Получаем словарь 
    '''
    name_db = name_from_time_db(time_min_ago)
    cup = read_sql(sql= f'{name_db}', con = connection, index_col = 'figi')
    data_cup = cup.loc[f'{figi}']
    dict_cup = data_cup.to_dict()
    return dict_cup


def name_from_time_db(time_min_ago):
    '''Получаем строчка типа datatime, возвращает имя бд
    '''
    date_time = time_min_ago.strftime("%H%M")
    date_time_str = f'cup{date_time}'
    return date_time_str


def chek_anomaly(time_min_ago, connection): 
    download_cup(connection)                                                                                 #todo
    list_candles_now = check_secid()                
    for secid, j in list_candles_now.items(): 
        data = read_sql(secid, connection)
        asyncio.run(anomaly_volume(data, j[3], secid, j[1], j[2], j[4], j[5], time_min_ago, connection))    #todo
        asyncio.run(anomaly_close(data, j[0], secid, j[1], j[2], j[4], j[3], j[5], time_min_ago,connection))    #todo
    delete_cup_two_min_ago(time_min_ago, connection)                                                               #todo



def update_history_data_bd(connection):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'db/test.db')
    os.remove(path)
    hist = GetHistoricalData()
    asyncio.run(hist.get_new_securities(connection))
    asyncio.run(hist.get_candles_history(connection))


def check_time(time_now):
    '''Проверка прошла ли минута с последнего запроса на получение текущего 
    '''
    time_now_start_min = datetime.datetime(time_now.year , time_now.month, time_now.day, time_now.hour, time_now.minute, 00)
    if (datetime.datetime.today() < time_now_start_min + datetime.timedelta(minutes=1)):
        sleep(3)
        return check_time(time_now_start_min)
    print('ok')


def check_exist_table_cup(time_min_ago, connection):
    name_db = name_from_time_db(time_min_ago)
    exist_table = table_exists(f'{name_db}', connection)
    return exist_table

def run_with_time_debug(first_run):
    engine = create_engine(r'sqlite:///db/test1.db')
    connection  = engine.connect()
    time_now = datetime.datetime.today() 
    time_min_ago = time_now - datetime.timedelta(minutes=1)
    str_time_min_ago = time_min_ago.strftime("%m-%d-%Y %H:%M:%S")
    print(str_time_min_ago)
    start_time = time.time()
    exist_table = check_exist_table_cup(time_min_ago, connection)
    if first_run == True or exist_table == False:
        download_cup(connection)
        check_time(time_now)
        return None
    chek_anomaly(time_min_ago, connection)                             #todo
    print("--- %s seconds ---" % (time.time() - start_time))
    check_time(time_now)
    connection.close()



if __name__ == "__main__":
    first_run = False
    while(True):
        run_with_time_debug(first_run)
