# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 12:33:33 2021

@author: DL
"""

import pandas as pd
import os


from constants import Constants
import matplotlib.pyplot as plt
import importlib

import utils

# 폰트 설정
from matplotlib import font_manager, rc
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rc('axes', unicode_minus=False)

importlib.reload(utils)



os.chdir(r'D:\데이터 분석\src')
data = utils.ElecData(
    # 파일 이름에 hive 또는 post가 포함되어야 함
    # fileName = 'post_lake_opm.tb_lp_mart_15mi.csv',
    fileName = 'post_tb_lp_mart_hour_2020.csv',
    # fileName = 'hive_tb_day_lp_30day_bfor_data_2020_210201.csv',
    # fileName = 'hive_tb_day_lp_30day_bfor_data_2020.csv',
    period = '2020-01-01:2020-12-31', # 좌측, 우측 모두 포함
    # busiDiv = ['산업'],
    # category = ['금속', '병원'], 
    # enteCode = ['055344', '000014'] # 하위기준이 우선순위가 높음
    )

hive = utils.ElecData(
    # 파일 이름에 hive 또는 post가 포함되어야 함
    # fileName = 'post_lake_opm.tb_lp_mart_15mi.csv',
    fileName = 'hive_tb_day_lp_30day_bfor_data_20200101_20201231.csv',
    # fileName = 'hive_tb_day_lp_30day_bfor_data_2020_210201.csv',
    # fileName = 'hive_tb_day_lp_30day_bfor_data_2020.csv',
    period = '2020-01-01:2020-12-31', # 좌측, 우측 모두 포함
    # busiDiv = ['산업'],
    # category = ['금속', '병원'], 
    # enteCode = ['055344', '000014'] # 하위기준이 우선순위가 높음
    )


df = viewHeader(myDir=r'D:\데이터 분석\data\postGres', 
            fileName = 'post_tb_asos_mart_hour_20200101_20201231.csv',
            reload = True, pickleWrite=False, nrows=2)
    
    

cust_no_lst_api_df = utils.load_json(path = 'D:\데이터 분석\data',
                fileName = 'custLst_210216.txt')

importlib.reload(utils)
cust_no_lst_hive_df = utils.load_cust_no_list_hive(
    path = 'D:\데이터 분석\data',
    fileName = 'hive_tb_cust_no_list_20201201_20210217.csv',
    date = '20210216')



cust_no_lst_hive_set = set(cust_no_lst_hive_df.cust_no)

cust_no_lst_api_set = set(cust_no_lst_api_df.custNo)

print(cust_no_lst_hive_set  - cust_no_lst_api_set )
print(cust_no_lst_api_set  - cust_no_lst_hive_set )


cust_no_df.to_excel('cust_no_df.xlsx')

data.data.columns.tolist()

# 부하율 계산(일 최대, 연 최대)

cons, lr_day, lr_year = utils.grpEnteLoadRate(data.data)



def plotConsLoadRate(cons, lr_day, lr_year, cust_no_lst):
    
    fig, ax1 = plt.subplots()
    
    # 제목
    title = ''

    # 고객번호에 대해 순환하면서 그림
    for cust_no in cust_no_lst:
        
        # cust_no = '0135429711'
        
        # 하나의 고객번호에 대해 그림을 그리도록 전치
        # multi index는 2수준의 값으로 정리
        lr_day_to_plot = lr_day.loc[(slice(None), cust_no), :].T
        lr_day_to_plot.index = lr_day_to_plot.index.get_level_values(1)
        
        lr_year_to_plot = lr_year.loc[(slice(None), cust_no), :].T
        lr_year_to_plot.index = lr_year_to_plot.index.get_level_values(1)
        
        cons_to_plot = cons.loc[(slice(None), cust_no), :].T
        cons_to_plot.index = cons_to_plot.index.get_level_values(1)
        
        # 그래프 출력
        ax1.plot(lr_day_to_plot, color = 'green', marker='.', label = (cust_no + '_dailyMax'))
        ax1.plot(lr_year_to_plot, color = 'blue', marker='*', label = (cust_no + '_yearlyMax'))
        ax1.set_ylabel('LoadRate(%)')
        
        ax2 = ax1.twinx()
        ax2.plot(cons_to_plot, '--r', label=(cust_no + '_consumption'))
        ax2.set_ylabel('Consumption')
        eachTitle = f'cust_no : {cust_no}, consumption : {np.nansum(cons_to_plot):,.0f}(kWh) \n'
        title = title + eachTitle
    
    plt.title(title)
    fig.legend()
    
    # plt.legend()
    plt.show()

# cust_no_lst = ['0135429711', '0141807613', '0135429686']        
cust_no_lst = ['0135429711']        

plotConsLoadRate(cons, lr_day, lr_year, cust_no_lst)
    

lr_day.loc[(slice(None), '0135429711'), :].T.plot()

def grpEnteLoadRate(df, data_type = 'post', ente_col = 'ente',
                cust_no_col = 'cust_no', date_col='meter_dd',
                elec_kwd = 'elcp'):



data_to_look = data.data.loc[mask]
elec_col = [col for col in data_to_look.columns if 'pwr_qty' in col]
data_to_look['mr_ymd'] = pd.to_datetime(data_to_look['mr_ymd'], format='%Y%m%d')
data_to_look['elecSum'] = data_to_look[elec_col].sum(axis=1)
data_grp = data_to_look.groupby('mr_ymd')['elecSum'].sum()

plt.plot(data_grp.index, data_grp)
data_grp.plot()

data_to_look_grp = data_to_look.groupby('part_key_mr_ymd')['ENTE_CODE'].count()
data_to_look_grp.to_excel('data_to_look_grp.xlsx')


ente = data.jijung.ENTE.unique()
len(ente)

# hive
mask1 = data2.data.part_key_mr_ymd =='2020-12-21'
ente_mask2 = data2.data[mask1].ENTE_CODE.unique()
print(f'필터 적용된 hive 전체 사업장 개수 : {data2.data[mask1].ENTE_CODE.nunique()}')
elec_cols = [col for col in data2.data.columns if 'pwr_qty' in col]
print(f'전력사용량 : {data2.data.loc[mask1, elec_cols].sum().sum():,}')



# post
mask1 = data.data.meter_dd =='2020-12-21'
# mask2 = data.data.part_name == '산업'


print(f'필터 적용된 postGres 전체 사업장 개수 : {data.data[mask].ente.nunique()}')
ente_mask = data.data[mask1].ente.unique()

elec_cols = [col for col in data.data.columns if 'elcp_use' in col]
print(f'전력사용량 : {data.data.loc[mask1, elec_cols].sum().sum():,}')


elecCols = [col for col in data.data.columns if 'elcp_use' in col]


data.data.loc[mask, elecCols].sum().sum()


data.data.ente.isin(['000014']).sum()

(data.data.ente == '000014').sum()

data.data.ente.nunique()



### 피크시각누적도

d_path = r'D:\0. 통계분석실(2021)\2. SEDA\0128 관리자 페이지 확인'
os.chdir(d_path)

peak = pd.read_excel('전력데이터분석_피크시각누적추이_세부내역_20210128.xlsx',
                     dtype={'업체코드':str})

peak_ente = peak['업체코드'].unique()
print('peak analysis')
print(f'unique ente : {len(peak_ente)}')
print(f'postGres 전체 사업장 개수 : {data.data.ente.nunique()}')


# 온도민감도 분석

d_path = r'D:\0. 통계분석실(2021)\2. SEDA\0128 관리자 페이지 확인'
os.chdir(d_path)

sensitivity = pd.read_csv('전력데이터분석_온도민감도분석_세부내역_20210128.csv',
                     dtype={'업체코드':str, 
                            '고객번호':str}, encoding='cp949')



# 데이터 개수 비교


ente_total = pd.DataFrame(ente_mask)
ente_total.rename(columns={0:'ente_code'}, inplace=True)
ente_total['postGres'] = 1
ente_total_df = ente_total.set_index('ente_code')

ente_mask2_df = pd.DataFrame(ente_mask2)
ente_mask2_df.rename(columns={0:'ente_code'}, inplace=True)
ente_mask2_df['hive'] = 1
ente_mask2_df = ente_mask2_df.set_index('ente_code')


ente_total_df_mrg = pd.concat([ente_total_df, ente_mask2_df], axis=1)
ente_total_df_mrg.sort_index(inplace=True)
ente_total_df_mrg.to_excel('ente_total_df_mrg.xlsx')



ente_jijung_df = pd.DataFrame(ente)
ente_jijung_df.rename(columns={0:'ente_code'}, inplace=True)
ente_jijung_df['다소비사업장'] = 1
ente_jijung_df = ente_jijung_df.set_index('ente_code')


ente_total_df_mrg2 = pd.concat([ente_total_df_mrg, ente_jijung_df], axis=1)
ente_total_df_mrg2.sort_index(inplace=True)
ente_total_df_mrg2.to_excel('ente_total_df_mrg2.xlsx')


# 데이터 카랄로그

d_path = r'D:\0. 통계분석실(2021)\2. SEDA\0128 관리자 페이지 확인'
os.chdir(d_path)

sensitivity = pd.read_csv('한국전력공사수집데이터_20210128.csv',
                     dtype={'업체코드':str, 
                            '고객번호':str}, encoding='cp949')

with open('한국전력공사수집데이터_20210128.csv', 'r') as f:
    txtData = f.read()

txtData = txtData.replace('\t', '')

oneLine = []

for line in txtData.split('\n'):
    oneLine.append(line.split(','))

oneLine_df = pd.DataFrame(oneLine[1:], columns = oneLine[0])    
    
oneLine_df.dtypes
eleccols = [col for col in oneLine_df.columns if '시' in col]
oneLine_df[eleccols] = oneLine_df[eleccols].astype('float')

mask = oneLine_df['날짜'] == '2020-12-21'
oneLine_df.loc[mask, eleccols].to_excel('oneLine_df.xlsx')





'한국전력공사수집데이터_20210128.csv'

# 데이



set_data_ente = set(data.data.ENTE_CODE)
set_data_ente

exem = pd.read_csv('..\\data\\임시\\업체코드_전력데이터형황-데이터수집현황.csv', 
                   dtype={'ente_code':str})




set_exem = set(exem.ente_code)
set_exem

diff = set_data_ente - set_exem

len(diff)
pd.DataFrame(diff).to_excel('..\\data\\임시\\diff.xlsx')

import pandas as pd
test = pd.read_pickle('..\\data\\hive\\hive_tb_day_lp_30day_bfor_data_2020.pickle')
os.getcwd()






