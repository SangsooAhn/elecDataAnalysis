# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 09:53:58 2021

@author: DL
"""


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import utils 
from constants import Constants
import os
import seaborn as sns
import re
from pdb import set_trace as s
from datetime import datetime

from matplotlib import font_manager, rc
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rc('axes', unicode_minus=False)

def clrDuplicateRecords(path, fileName,
                        ref_col = '고객번호', pick_col = '정보제공 동의일'):
    
    '''
    edsm 정보제공동의 중복 데이터 정리
    
    test
    ------
    path = path
    fileName = Constants.EDSM_AGREEMENT
    ref_col = '고객번호'
    pick_col = '정보제공 동의일'
    
    
    '''

    exists, path = utils.chkFileExists(path, fileName)
    
    # 파일이 존재하는 경우
    if exists:
        df = pd.read_excel(os.path.join(path, fileName), skiprows=1)
        
        # 중복데이터 처리
        duplicates = df[df[ref_col].duplicated(keep=False)]
        
        # 원데이터에서 삭제
        idx_delete = duplicates.index
        df = df.drop(idx_delete, axis=0)

        lst_add = []
        
        # 가장 최근 데이터만 추출
        for cust_no in duplicates[ref_col].unique():
            # cust_no = 338613165
            mask = duplicates[ref_col] == cust_no
            
            # 가장 정보제공 동의일이 최신인 데이터를 선택
            idx_pick = duplicates.loc[mask, pick_col].sort_values().index[-1]
            keep = duplicates.loc[idx_pick]
            lst_add.append(keep)
        
        df_add = pd.DataFrame(lst_add)
        
        df_tot = pd.concat([df, df_add])
    
    return df_tot


def unqRecords(df, cols = ['cust_no', 'date']):
    '''
    df에서 cols가 고유한 데이터 반환
    
    test
    ----
    df = df_new
    cols = ['cust_no', 'date']
    
    '''

    # 고객번호, 동의날짜 list of tuple, index는 제외하므로 slice는 1부터 시작
    lst = [row[1:] for row in df[cols].itertuples()]  
    return set(lst)

def findWeek(x):
    '''
    YYYY-MM-DD 형태의 str에서 week 정보 추출
    
    test
    ----
    x = '2020-04-35'
    '''
    day = x[-2:] 
    day_int = int(day)
    
    return (day_int // 7) + 1
    
if __name__ == '__main__':

    # 파일 로드
    relPath = '..\\data'
    
    # 신규 파일
    fileName = Constants.EDSM_AGREEMENT
    df_new = clrDuplicateRecords(relPath, fileName,
                        ref_col = '고객번호', pick_col = '정보제공 동의일')  
    # 기존 파일
    fileName_old = Constants.EDSM_AGREEMENT_OLD
    df_old = clrDuplicateRecords(relPath, fileName_old,
                        ref_col = '고객번호', pick_col = '정보제공 동의일')  
    
    df_tot = pd.merge(df_new, df_old, on=['고객번호', '제공기간 시작일'], 
                      how='outer')
    
    # excel 파일 쓰기 오류를 발생하는 단어 제거
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
    df_tot = df_tot.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)
    
    # df_tot.to_csv('df_tot.csv')
    # df_tot.to_excel('df_tot.xlsx')
    
    df_new['year'] = df_new['제공기간 종료일'].str[:4]
    df_new['month'] = df_new['제공기간 종료일'].str[5:7]
    df_new['week'] = df_new['제공기간 종료일'].map(lambda x:findWeek(x))

    grp_size = df_new.groupby(['year', 'month', 'week']).size()
    grp_size.name = 'num_cust_no'
    
    grp_size_df = grp_size.reset_index()
    
    grp_size_df['idx'] = grp_size_df['year'].str[2:] + '_' \
        + grp_size_df['month'] + '_' \
            + grp_size_df['week'].astype(str)
            
    today = datetime.strftime(datetime.today(), '%Y-%m-%d')
    
    plt.figure()
    plt.plot(grp_size_df['idx'], grp_size_df['num_cust_no'], marker='*', linestyle='none')
    plt.bar(grp_size_df['idx'], grp_size_df['num_cust_no'])
    # sns.histplot(x='idx', y='num_cust_no', data = grp_size_df)
    plt.title(f"한전 정보제공동의 만료 현황 \n (기준날짜 : {today}, 전체 : {np.sum(grp_size_df['num_cust_no']):,}개)")
    plt.ylabel('고객번호 개수(개)')
    plt.xlabel('종료시점')
    plt.xticks(rotation=90)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['top'].set_visible(False)