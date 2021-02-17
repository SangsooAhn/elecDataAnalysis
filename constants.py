# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 12:19:26 2021

@author: DL
"""

'''
21/1/26

신고 테이블 추가 및 수

'''



import pandas as pd
import numpy as np
from configparser import ConfigParser
    


def hiveTimeColsSet():
    '''
    hive로 저장되는 전력데이터에 해당하는 컬럼
    '''
    
    hiveTimeCols = [
        'pwr_qty'+f'{i}'.zfill(2)+f'{j}' 
        for i in range(24) 
        for j in ['00', '15', '30', '45']
        ]
    hiveTimeCols.remove('pwr_qty0000')
    hiveTimeCols = hiveTimeCols + ['pwr_qty2400']
    
    return hiveTimeCols
hiveTimeCols = hiveTimeColsSet()

def postGresTimeColsSet():
    postGresTimeCols = [
        'tm_'+f'{i}'.zfill(2)
        for i in range(24)]
    
    return postGresTimeCols
postGresTimeCols = postGresTimeColsSet()

class Constants:
    # Unit conversions
    S_PATH = r'G:\python_dev\빅데이터 기반 지능형 에너지관리시스템\elecAnalProj'
    # S_PATH = r'G:\python_dev\빅데이터 기반 지능형 에너지관리시스템\elecAnalProj'
    # RELOAD = False
    RELOAD = True
    

    ## 에너지사용량신고
    # 제출 여부, 시도, 업종, 연도, 에너지사용량 등
    JIJUNG = 'JIJUNG_S_2019.xlsx'

    JIJUNG_USE_COLS = ['ENTE', 'SIDO', 'KEMC_OLDX_CODE', 'YEAR', 'E1', 
                           'SUMM2', 'KSIC9_FCODE', 'KSIC9_2CODE', 'ENTE_SCTX']

    JIJUNG_USE_COLS_ORI = ['ENTE', 'SIDO', 'KEMC_OLDX_CODE', 'JUL_Y', 'JUL_Y2', 
                       'JUL_PY', 'JUL_PY2', 'JUL_A', 'JUL_PA', 'YUNG', 'YUNGP', 
                       'JA', 'JAP', 'YEAR', 'A1', 'AP1', 'A2', 'AP2', 'A3', 
                       'AP3', 'A4', 'AP4', 'A5', 'AP5', 'A6', 'AP6', 'A7',
                       'AP7', 'A8', 'AP8', 'AT1', 'APT1', 'B1', 'BP1', 'B2', 
                       'BP2', 'B3', 'BP3', 'B4', 'BP4', 'B5', 'BP5', 'B6',
                       'BP6', 'B7', 'BP7', 'BT1', 'BPT1', 'C1', 'CP1', 'C2',
                       'CP2', 'C3', 'CP3', 'C4', 'CP4', 'CT1', 'CPT1', 'D1',
                       'DP1', 'D2', 'DP2', 'D3', 'DP3', 'D4', 'DP4', 'DT1',
                       'DPT1', 'E1', 'EP1', 'E2', 'EP2', 'E3', 'EP3', 'ET1'
                       , 'EPT1', 'SUMM', 'SUMP', 'SUMM2', 'SCALE', 'C1_IN',
                       'C1_OUT', 'CP1_IN', 'CP1_OUT', 'C2_IN', 'C2_OUT',
                       'CP2_IN', 'CP2_OUT', 'C5', 'CP5', 'B8', 'BP8', 'GHG',
                       'KSIC9_FCODE', 'KSIC9_2CODE', 'A9', 'AP9', 'A10', 
                       'AP10', 'A11', 'AP11', 'A12', 'AP12', 'A13', 'AP13',
                       'A14', 'AP14', 'A15', 'AP15', 'A16', 'AP16', 'TE',
                       'WRIT_MANX', 'WRIT_DAYX', 'ENTE_SCTX', 'B9', 'BP9']
    
    # 한전 고객번호 등
    TB_OPM_001 = 'tb_opm_001.xlsx'
    TB_OPM_001_USE_COLS = ['ENTE_CODE', 'CUST_NO', 'CNTR_ELCP', 'MAX_LOAD']

    
    # 업체코드, 업체명, 업종, 시도
    TB_A01_018 = 'tb_a01_018_210126.xlsx'
    TB_A01_018_USE_COLS = ['ENTE_CODE', 'ENTE_TITE', 'KEMC_OLDX_CODE', 'CITY_PROV_CODE']
    
    TB_A01_018_USE_COLS_ORI = ['ENTE_CODE', 'BUSI_DIVD_CODE', 'CITY_PROV_CODE',
                               'GOVE_CODE', 'ENTE_STTE_DIVD_CODE', 'KEMC_NEWX_CODE',
                               'KEMC_OLDX_CODE', 'TSTD_TOIN_CODE', 'ENTE_TITE', 
                               'REPE_MANX_TITE', 'ENTR_REGI_NUMB', 'MAOF_POST_NUMB', 
                               'MAOF_RESE', 'MAOF_PHON_NUMB', 'MAOF_FAXX_NUMB', 
                               'OFFI_POST_NUMB', 'OFFI_RESE', 'OFFI_PHON_NUMB', 
                               'CHAR_MANX_TITE', 'CHAR_MANX_RESP', 'CHAR_MANX_STAT', 
                               'CHAR_MANX_PHON_NUMB', 'CHAR_MANX_EMAL', 'CHAR_MANX_FAXX',
                               'CODE_STAT_FLAG', 'WRIT_MANX', 'WRIT_DAYX', 
                               'PASSWD', 'CONT_YN', 'MAIL_YN', 'MAIL_DATE', 
                               'SMS_YN', 'SMS_DATE', 'VA_GRP', 'VA_YEAR', 
                               'CORP_NUMB', 'KSIC9_FCODE', 'PW_BC', 'MAOF_RESE_DTLS',
                               'OFFI_RESE_DTLS', 'CHAR_MANX_HAND_NUMB', 
                               'MODI_MANX', 'MODI_DAYX', 'ENTE_SCTX', 
                               'INDU_ESTA_YN', 'INDU_ESTA_CODE', 'KSIC10_FCODE',
                               'CHAN_INPT_GUBN', 'KEMC_SECT']

    busiCatDict = {
        '310' : '식품', '320' : '섬유', '330' : '제지목재', '350' : '화공',
        '360' : '요업', '370' : '금속', '390' : '산업기타', '410' : '발전',
        '501' : '상용', '502' : '공공', '503' : '아파트', '504' : '호텔', 
        '505' : '병원', '506' : '학교', '507' : 'IDC(전화국)',
        '508' : '연구소', '509' : '백화점', '599' : '건물기타',
        '510' : '목욕탕', '511' : '군부대', '999' : '업종기타',
        '701' : '개인', '512' : '수송', '500' : '건물',
        '610' : '육상운송', '620' : '철도운송', '630' : '해운운송',
        '640' : '항공운송', '650' : '항만·하역', '380' : '에너지산업'
        }    

    divCatDict1 = {code:'산업' for code in busiCatDict.keys() if code[0]=='3'}
    divCatDict2 = {code:'발전' for code in busiCatDict.keys() if code[0]=='4'}
    divCatDict3 = {code:'건물' for code in busiCatDict.keys() if code[0]=='5'}
    divCatDict4 = {code:'수송' for code in busiCatDict.keys() if code[0]=='6'}
                  
    divCatDict = {**divCatDict1, 
                  **divCatDict2, 
                  **divCatDict3, 
                  **divCatDict4}
    
    SIDODict = {
        '22' : '대구', '23' : '인천', '24' : '광주', '25' : '대전', 
        '26' : '울산', '31' : '경기', '32' : '강원', '33' : '충북', 
        '34' : '충남', '35' : '전북', '36' : '전남', '37' : '경북', 
        '38' : '경남', '39' : '제주', '11' : '서울', '21' : '부산', 
        '99' : '황북', '12' : '세종'}
    
    ENTE_SCTX_dict = {
        '1':'대기업', 
        '2':'중견기업', 
        '3':'중소기업',
        '4':'공공기관',
        '9':'해당없음'
        }


    # postGres     
    POSTGRES = 'elec_weather_cleaned_2019Jul_2020Sep.csv'
    POSTGRES_USE_COLS = ['ente_code', 'cust_no', 'cntr_tp', 'meter_no', 'dt', 'tm_00', 'tm_01', 'tm_02', 'tm_03', 'tm_04', 'tm_05', 'tm_06', 'tm_07', 'tm_08', 'tm_09', 'tm_10', 'tm_11', 'tm_12', 'tm_13', 'tm_14', 'tm_15', 'tm_16', 'tm_17', 'tm_18', 'tm_19', 'tm_20', 'tm_21', 'tm_22', 'tm_23']
    POSTGRES_TIME_COLS = postGresTimeCols

    POSTGRES_USE_COLS_ORI = ['part_code_new', 'part_name', 'kemc_oldx_code',
                             'kemc_oldx_code_tite', 'ente', 'cust_no', 
                             'cntr_tp_code', 'cntr_tp_name', 'meter_no',
                             'season_code', 'season_name', 'weekd_weekend_code',
                             'weekd_weekend_name', 'meter_dd', 'elcp_use_0000',
                             'elcp_use_0015', 'elcp_use_0030', 'elcp_use_0045',
                             'elcp_use_0100', 'elcp_use_0115', 'elcp_use_0130',
                             'elcp_use_0145', 'elcp_use_0200', 'elcp_use_0215',
                             'elcp_use_0230', 'elcp_use_0245', 'elcp_use_0300',
                             'elcp_use_0315', 'elcp_use_0330', 'elcp_use_0345',
                             'elcp_use_0400', 'elcp_use_0415', 'elcp_use_0430',
                             'elcp_use_0445', 'elcp_use_0500', 'elcp_use_0515',
                             'elcp_use_0530', 'elcp_use_0545', 'elcp_use_0600',
                             'elcp_use_0615', 'elcp_use_0630', 'elcp_use_0645',
                             'elcp_use_0700', 'elcp_use_0715', 'elcp_use_0730',
                             'elcp_use_0745', 'elcp_use_0800', 'elcp_use_0815',
                             'elcp_use_0830', 'elcp_use_0845', 'elcp_use_0900',
                             'elcp_use_0915', 'elcp_use_0930', 'elcp_use_0945',
                             'elcp_use_1000', 'elcp_use_1015', 'elcp_use_1030',
                             'elcp_use_1045', 'elcp_use_1100', 'elcp_use_1115',
                             'elcp_use_1130', 'elcp_use_1145', 'elcp_use_1200',
                             'elcp_use_1215', 'elcp_use_1230', 'elcp_use_1245',
                             'elcp_use_1300', 'elcp_use_1315', 'elcp_use_1330',
                             'elcp_use_1345', 'elcp_use_1400', 'elcp_use_1415',
                             'elcp_use_1430', 'elcp_use_1445', 'elcp_use_1500',
                             'elcp_use_1515', 'elcp_use_1530', 'elcp_use_1545',
                             'elcp_use_1600', 'elcp_use_1615', 'elcp_use_1630',
                             'elcp_use_1645', 'elcp_use_1700', 'elcp_use_1715',
                             'elcp_use_1730', 'elcp_use_1745', 'elcp_use_1800',
                             'elcp_use_1815', 'elcp_use_1830', 'elcp_use_1845',
                             'elcp_use_1900', 'elcp_use_1915', 'elcp_use_1930',
                             'elcp_use_1945', 'elcp_use_2000', 'elcp_use_2015',
                             'elcp_use_2030', 'elcp_use_2045', 'elcp_use_2100',
                             'elcp_use_2115', 'elcp_use_2130', 'elcp_use_2145',
                             'elcp_use_2200', 'elcp_use_2215', 'elcp_use_2230',
                             'elcp_use_2245', 'elcp_use_2300', 'elcp_use_2315',
                             'elcp_use_2330', 'elcp_use_2345']
 
   
    # hive
    HIVE_TIME_COLS = hiveTimeCols
    HIVE_ELEC_TABLE = 'tb_day_lp_30day_bfor_data.'    
    
    
    # EDSM

    parser = ConfigParser()
    parser.read('config.ini')
    
    EDSM_CUSTNO_API = 'EDSM_CustNo_API_20210124_175915.xlsx'
    # AGREEMENT = '고객정보제공 동의현황_210111.xls'
    # EDSM_AGREEMENT = '고객정보제공 동의현황_210124.xls'
    # EDSM_AGREEMENT = '고객정보제공 동의현황_210124a.xls'
    EDSM_AGREEMENT = '고객정보제공 동의현황_210218.xls'
    EDSM_AGREEMENT_OLD = '고객정보제공 동의현황_210111.xls'
    
    # API 사용을 위한 key load
    EDSM_API_KEY = parser.get('EDSM', 'api_key')    


