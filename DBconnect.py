import pandas as pd
import psycopg2
import os
from pdb import set_trace as s
from datetime import datetime 
import time

from constants import Constants
from configparser import ConfigParser

parser = ConfigParser()
parser.read('config.ini')

POST_HOST = parser.get('POST', 'POST_HOST')
POST_DB = parser.get('POST', 'POST_DB')
POST_USER = parser.get('POST', 'POST_USER')
POST_PASSWORD = parser.get('POST', 'POST_PASSWORD')

HIVE_HOST = parser.get('HIVE', 'HIVE_HOST')
HIVE_PORT = parser.get('HIVE', 'HIVE_PORT')
HIVE_USER = parser.get('HIVE', 'HIVE_USER')
HIVE_PASSWORD = parser.get('HIVE', 'HIVE_PASSWORD')
HIVE_DB = parser.get('HIVE', 'HIVE_DB')

'''
21-02-15 용량이 큰 데이터를 나눠서 받을 수 있는 함수 추가


'''


def timer(func):
    
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        total = time.time() - start
        print('소요시간 {:.1f}s\n'.format(total))
        return result
    return wrapper

def postGresDownload(sql, dataPath, fileName):


    '''
    특정 sql을 postGres에서 실행 후 해당 결과를 파일로 저장
    '''

    # postGres 접속
    with psycopg2.connect(
        host=POST_HOST,
        database=POST_DB,
        user=POST_USER,
        password=POST_PASSWORD
        ) as conn:
        
        # s()
        df = pd.read_sql(sql, conn)
        # print(df)
        # s()
        df.to_csv(os.path.join(dataPath, fileName))
    
    return df



# sql = '''
# select * 
# from lake_weath.tb_srcv_asos_hour_mart
# where lake_weath.tb_srcv_asos_hour_mart.meter_dd >= '20201201'
# and lake_weath.tb_srcv_asos_hour_mart.meter_dd <= '20210130';
# '''

import pandas as pd
from pyhive import hive
import puretransport

# sql = """select * from opm.tb_day_lp_4day_bfor_data where
#         part_key_mr_ymd > '20201201' and 
#         part_key_mr_ymd < '20210130'"""


def hiveDownload(sql, dataPath, fileName, **kwargs):
    '''
    hive에 접속하여 특정 sql을 실행하고 해당 결과를 저장
    '''
    transport = puretransport.transport_factory(
        host=HIVE_HOST, 
        port=HIVE_PORT,
        username=HIVE_USER,
        password=HIVE_PASSWORD)

    with hive.Connection(thrift_transport=transport,
                         database=HIVE_DB,
                         username=HIVE_USER) as conn:
        df = pd.read_sql(sql, conn)
        # s()
        # df = pd.read_sql("show tables", conn)
        # print(df)
        df.to_csv(os.path.join(dataPath, fileName), **kwargs)

@timer
def get_post_data(schema = 'lake_weath',
                  tableName = 'tb_asos_mart_hour',
                  dateCol = 'dt',
                  startDate = '20200101',
                  endDate = '20201231',
                  numDiv = 4, **kwargs
                  ):

    '''
    post에서 필요한 데이터 받기
    메모리 문제로 나눠서 받고 통합
    
    test
    ------
    schema = 'lake_weath'
    tableName = 'lake_weath.tb_asos_mart_hour',
    dateCol = 'dt'
    startDate = '20200101'
    endDate = '20200131'
    numDiv = 4

    '''
    
    startDate_dt = pd.to_datetime(startDate, format='%Y%m%d')
    endDate_dt = pd.to_datetime(endDate, format='%Y%m%d')
    dates = pd.date_range(start=startDate_dt, 
                            end=endDate_dt, freq = 'D')
    
    period = (dates[-1] - dates[0] + pd.Timedelta('1d')).days
    span = int(period/numDiv)
    
    
    starts_dt = [date for i, date in enumerate(dates) if i%span == 0]
    ends_dt = [date - pd.Timedelta('1d') for date in starts_dt[1:] if date != endDate_dt]
    ends_dt.append(endDate_dt)
    
    starts = [datetime.strftime(date, format='%Y-%m-%d') for date in starts_dt]
    ends = [datetime.strftime(date, format='%Y-%m-%d') for date in ends_dt]
    
    # 나누지 않는다면
    if numDiv ==1:
        starts = [startDate]
        ends = [endDate]
        

    for start, end in zip(starts, ends):
        
        fileName = f'post_{tableName}_{start}_{end}.csv'
        
        sql = ''
        sql += f"select * from {schema}.{tableName} where\n "
        sql += f"{dateCol} >= '{start}' and\n "
        sql += f"{dateCol} <= '{end}'"

        os.chdir(r'D:\데이터 분석\src')
        df = postGresDownload(sql, dataPath = '..\\data\\postGres', 
                              fileName = fileName, **kwargs)
    
    # 통합
    df_tot = []
    for start, end in zip(starts, ends):
        
        fileName = f'post_{tableName}_{start}_{end}.csv'
        os.chdir(r'D:\데이터 분석\src')
        df = pd.read_csv('..\\data\\postGres\\'+fileName)
        df_tot.append(df)

    fileName = f'post_{tableName}_{startDate}_{endDate}.csv'
    df = pd.concat(df_tot, axis=0)
    df = df.drop('Unnamed: 0', axis=1)            
    df.to_csv('..\\data\\postGres\\'+fileName, index=False, **kwargs)

@timer
def get_tb_lp_mart_15mi():

    # 15분 주기 데이터 받기 tb_lp_mart_15mi # 1
                
    sql = '''
    select * 
    from lake_opm.tb_lp_mart_15mi
    where lake_opm.tb_lp_mart_15mi.meter_dd >='20200101'
    and lake_opm.tb_lp_mart_15mi.meter_dd <='20200630';
        '''
    os.chdir(r'D:\데이터 분석\src')
    df = postGresDownload(sql, dataPath = '..\\data\\postGres', 
                          fileName = 'post_tb_lp_mart_15mi_2020A.csv')
    
    
    # 15분 주기 데이터 받기 tb_lp_mart_15mi # 2
    
    sql = '''
    select * 
    from lake_opm.tb_lp_mart_15mi
    where lake_opm.tb_lp_mart_15mi.meter_dd >='20200701'
    and lake_opm.tb_lp_mart_15mi.meter_dd <='20201231';
        '''
    os.chdir(r'D:\데이터 분석\src')
    df = postGresDownload(sql, dataPath = '..\\data\\postGres', 
                          fileName = 'post_tb_lp_mart_15mi_2020B.csv')

    df1 = pd.read_csv('..\\data\\postGres\\post_tb_lp_mart_15mi_2020A.csv')
    df2 = pd.read_csv('..\\data\\postGres\\post_tb_lp_mart_15mi_2020B.csv')
    
    df3 = pd.concat([df1, df2], axis=0)
    df3 = df3.drop('Unnamed: 0', axis=1)
    df3.to_csv('..\\data\\postGres\\post_tb_lp_mart_15mi_2020.csv', index=False)


# @timer
# def get_hive_data(tableName = 'tb_day_lp_30day_bfor_data',
#                   dateCol = 'part_key_mr_ymd',
#                   startDate = '20200101',
#                   endDate = '20201231',
#                   numDiv = 4
#                   ):

#     '''
#     hive에서 필요한 데이터 받기
#     메모리 문제로 나눠서 받고 통합
    
#     test
#     ------
    
#     tableName = 'tb_day_lp_30day_bfor_data'
#     dateCol = 'part_key_mr_ymd'
#     startDate = '20200101'
#     endDate = '20201231'
#     startDate2 = '20200701'
#     endDate2 = '20201231'
#     numDiv = 3
    
#     '''
    
#     startDate_dt = pd.to_datetime(startDate, format='%Y%m%d')
#     endDate_dt = pd.to_datetime(endDate, format='%Y%m%d')
#     dates = pd.date_range(start=startDate_dt, 
#                             end=endDate_dt, freq = 'D')
    
#     period = (dates[-1] - dates[0] + pd.Timedelta('1d')).days
#     span = int(period/numDiv)
    
    
#     starts_dt = [date for i, date in enumerate(dates) if i%span == 0]
#     ends_dt = [date - pd.Timedelta('1d') for date in starts_dt[1:] if date != endDate_dt]
#     ends_dt.append(endDate_dt)
    
#     starts = [datetime.strftime(date, format='%Y%m%d') for date in starts_dt]
#     ends = [datetime.strftime(date, format='%Y%m%d') for date in ends_dt]
    
#     # 나누지 않는다면
#     if numDiv ==1:
#         starts = [startDate]
#         ends = [endDate]
        

#     for start, end in zip(starts, ends):
        
#         fileName = f'hive_{tableName}_{start}_{end}.csv'
        
#         sql = ''
#         sql += f"select * from opm.{tableName} where\n "
#         sql += f"{dateCol} >= '{start}' and\n "
#         sql += f"{dateCol} <= '{end}'"

#         os.chdir(r'D:\데이터 분석\src')
#         df = hiveDownload(sql, dataPath = '..\\data\\hive', 
#                               fileName = fileName)
    
#     # 통합
#     df_tot = []
#     for start, end in zip(starts, ends):
        
#         fileName = f'hive_{tableName}_{start}_{end}.csv'
#         os.chdir(r'D:\데이터 분석\src')
#         df = pd.read_csv('..\\data\\hive\\'+fileName)
#         df_tot.append(df)

#     fileName = f'hive_{tableName}_{startDate}_{endDate}.csv'
#     df = pd.concat(df_tot, axis=0)
#     df = df.drop('Unnamed: 0', axis=1)            
#     df.to_csv('..\\data\\hive\\'+fileName, index=False)

#     # # hive 15분 주기 30일 경과 데이터 받기 # 1
#     # year = startDate1[:4]
                
#     # fileNameA = 'hive_{tableName}_{year}A.csv'
#     # fileNameB = 'hive_{tableName}_{year}B.csv'
#     # fileName = 'hive_{tableName}_{year}.csv'
    
    
#     # # sql = """select * from opm.tb_day_lp_30day_bfor_data where
#     # #         part_key_mr_ymd >= '20200101' and 
#     # #         part_key_mr_ymd <= '20200630';"""
            
    
#     # sql = ''
#     # sql += f"select * from opm.{tableName} where\n "
#     # sql += f"{dateCol} >= '{startDate1}' and\n "
#     # sql += f"{dateCol} <= '{endDate1}'"
    
#     # os.chdir(r'D:\데이터 분석\src')
#     # df = hiveDownload(sql, dataPath = '..\\data\\hive', 
#     #                       fileName = fileNameA)
    
    
#     # # hive 15분 주기 30일 경과 데이터 받기 # 2
    
#     # # sql = """select * from opm.tb_day_lp_30day_bfor_data where
#     # #         part_key_mr_ymd >= '20200701' and 
#     # #         part_key_mr_ymd <= '20201231';"""

#     # sql = ''
#     # sql += f"select * from opm.{tableName} where\n "
#     # sql += f"{dateCol} >= '{startDate2}' and\n "
#     # sql += f"{dateCol} <= '{endDate2}'"


#     # os.chdir(r'D:\데이터 분석\src')
#     # df = hiveDownload(sql, dataPath = '..\\data\\hive', 
#     #                       fileName = fileNameB)

#     # df1 = pd.read_csv('..\\data\\hive\\'+fileNameA)
#     # df2 = pd.read_csv('..\\data\\hive\\'+fileNameB)
    
#     # df3 = pd.concat([df1, df2], axis=0)
#     # df3 = df3.drop('Unnamed: 0', axis=1)
#     # df3.to_csv('..\\data\\hive\\'+fileName, index=False)

#     # hive data 다운받기

#     # sql = """select * from opm.tb_day_lp_30day_bfor_data where
#     #         part_key_mr_ymd >= '20200101' and 
#     #         part_key_mr_ymd <= '20201231'"""   


@timer
def get_hive_data(tableName = 'tb_day_lp_30day_bfor_data',
                  dateCol = 'part_key_mr_ymd',
                  startDate = '20200101',
                  endDate = '20201231',
                  numDiv = 4, **kwargs
                  ):

    '''
    hive에서 필요한 데이터 받기
    메모리 문제로 나눠서 받고 통합
    
    test
    ------
    
    tableName = 'tb_day_lp_30day_bfor_data'
    dateCol = 'part_key_mr_ymd'
    startDate = '20200101'
    endDate = '20201231'
    startDate2 = '20200701'
    endDate2 = '20201231'
    numDiv = 3
    
    '''
    
    startDate_dt = pd.to_datetime(startDate, format='%Y%m%d')
    endDate_dt = pd.to_datetime(endDate, format='%Y%m%d')
    dates = pd.date_range(start=startDate_dt, 
                            end=endDate_dt, freq = 'D')
    
    period = (dates[-1] - dates[0] + pd.Timedelta('1d')).days
    span = int(period/numDiv)
    
    
    starts_dt = [date for i, date in enumerate(dates) if i%span == 0]
    ends_dt = [date - pd.Timedelta('1d') for date in starts_dt[1:] if date != endDate_dt]
    ends_dt.append(endDate_dt)
    
    starts = [datetime.strftime(date, format='%Y%m%d') for date in starts_dt]
    ends = [datetime.strftime(date, format='%Y%m%d') for date in ends_dt]
    
    # 나누지 않는다면
    if numDiv ==1:
        starts = [startDate]
        ends = [endDate]
        

    for start, end in zip(starts, ends):
        
        fileName = f'hive_{tableName}_{start}_{end}.csv'
        
        sql = ''
        sql += f"select * from opm.{tableName} where\n "
        sql += f"{dateCol} >= '{start}' and\n "
        sql += f"{dateCol} <= '{end}'"

        os.chdir(r'D:\데이터 분석\src')
        df = hiveDownload(sql, dataPath = '..\\data\\hive', 
                              fileName = fileName, **kwargs)
    
    # 통합
    df_tot = []
    for start, end in zip(starts, ends):
        
        fileName = f'hive_{tableName}_{start}_{end}.csv'
        os.chdir(r'D:\데이터 분석\src')
        df = pd.read_csv('..\\data\\hive\\'+fileName)
        df_tot.append(df)

    fileName = f'hive_{tableName}_{startDate}_{endDate}.csv'
    df = pd.concat(df_tot, axis=0)
    df = df.drop('Unnamed: 0', axis=1)            
    df.to_csv('..\\data\\hive\\'+fileName, index=False, **kwargs)

    # # hive 15분 주기 30일 경과 데이터 받기 # 1
    # year = startDate1[:4]
                
    # fileNameA = 'hive_{tableName}_{year}A.csv'
    # fileNameB = 'hive_{tableName}_{year}B.csv'
    # fileName = 'hive_{tableName}_{year}.csv'
    
    
    # # sql = """select * from opm.tb_day_lp_30day_bfor_data where
    # #         part_key_mr_ymd >= '20200101' and 
    # #         part_key_mr_ymd <= '20200630';"""
            
    
    # sql = ''
    # sql += f"select * from opm.{tableName} where\n "
    # sql += f"{dateCol} >= '{startDate1}' and\n "
    # sql += f"{dateCol} <= '{endDate1}'"
    
    # os.chdir(r'D:\데이터 분석\src')
    # df = hiveDownload(sql, dataPath = '..\\data\\hive', 
    #                       fileName = fileNameA)
    
    
    # # hive 15분 주기 30일 경과 데이터 받기 # 2
    
    # # sql = """select * from opm.tb_day_lp_30day_bfor_data where
    # #         part_key_mr_ymd >= '20200701' and 
    # #         part_key_mr_ymd <= '20201231';"""

    # sql = ''
    # sql += f"select * from opm.{tableName} where\n "
    # sql += f"{dateCol} >= '{startDate2}' and\n "
    # sql += f"{dateCol} <= '{endDate2}'"


    # os.chdir(r'D:\데이터 분석\src')
    # df = hiveDownload(sql, dataPath = '..\\data\\hive', 
    #                       fileName = fileNameB)

    # df1 = pd.read_csv('..\\data\\hive\\'+fileNameA)
    # df2 = pd.read_csv('..\\data\\hive\\'+fileNameB)
    
    # df3 = pd.concat([df1, df2], axis=0)
    # df3 = df3.drop('Unnamed: 0', axis=1)
    # df3.to_csv('..\\data\\hive\\'+fileName, index=False)

    # hive data 다운받기

    # sql = """select * from opm.tb_day_lp_30day_bfor_data where
    #         part_key_mr_ymd >= '20200101' and 
    #         part_key_mr_ymd <= '20201231'"""   



if __name__ == '__main__':
    
    os.chdir(r'D:\데이터 분석\src')
    
    
    # 1시간 주기 데이터 받기 tb_lp_mart_hour
                
    sql = '''
    select * 
    from lake_opm.tb_lp_mart_hour
    where lake_opm.tb_lp_mart_hour.meter_dd >='20200101'
    and lake_opm.tb_lp_mart_hour.meter_dd <='20201231';
        '''
    os.chdir(r'D:\데이터 분석\src')
    df = postGresDownload(sql, dataPath = '..\\data\\postGres', 
                          fileName = 'post_tb_lp_mart_hour_2020.csv')
    


    get_hive_data(tableName = 'tb_day_lp_30day_bfor_data',
                  dateCol = 'part_key_mr_ymd',
                  startDate = '20200101',
                  endDate = '20201231',
                  numDiv = 4)
    
    
    
    get_hive_data(tableName = 'tb_cust_no_list',
                      dateCol = 'writ_dtm',
                      startDate = '20201201',
                      endDate = '20210217',
                      numDiv = 1, encoding='utf-8-sig'
                      )

    
    get_post_data(schema = 'lake_weath', 
                  tableName = 'tb_srcv_asos_hour_mart',
                  dateCol = 'dt',
                  startDate = '20200101',
                  endDate = '20201231',
                  numDiv = 4)    
  
    
    get_post_data(schema = 'lake_opm', 
                  tableName = 'tb_lp_mart_hour',
                  dateCol = 'meter_dd',
                  startDate = '20200101',
                  endDate = '20201231',
                  numDiv = 4)        