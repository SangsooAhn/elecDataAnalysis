# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 19:21:45 2021

@author: DL
"""

'''
20/2/4 함수 추가 (isRange)
20/1/26 테이블 로딩 등 수정


'''

import os
import pandas as pd
import urllib
import re
import pdb
import time
import requests
import numpy as np

import pandas as pd
import time
import datetime

# 한전 EDSM API 호출
import json

from pdb import set_trace as s
from constants import Constants

import matplotlib.pyplot as plt

# matplotlib 설정

SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIGGER_SIZE = 12
plt.rc('font', size=BIGGER_SIZE)
plt.rc('axes', titlesize=BIGGER_SIZE) # fontsize of the axes title 
plt.rc('axes', labelsize=BIGGER_SIZE) # fontsize of the x and y labels 
plt.rc('xtick', labelsize=BIGGER_SIZE) # fontsize of the tick labels 
plt.rc('ytick', labelsize=BIGGER_SIZE) # fontsize of the tick labels 
plt.rc('legend', fontsize=BIGGER_SIZE) # legend fontsize 
plt.rc('figure', titlesize=BIGGER_SIZE) # fontsize of the figure title


def doesFileExist(fileName):

    tempPath = os.getcwd()
    if os.path.exists(os.path.join(tempPath, fileName)):
        return True
    else:
        print(f'Check if "{fileName}" file exists')
        return False
        
        

def geoConvert(df, addr_col, geo_cols, rstFileName):
    
    '''
    naver 주소 API를 이용하여 도로명 주소, 위경도 등의 정보를 검색 후
    결과를 우측에 통합하여 반환
    '''

    # 주소 정보 변환
    geo = df[addr_col].map(searchMap)

    # 변환된 정보에 컬럼 정보 geo_cols를 지정하고 dataFrame으로 변환
    geo_df = pd.DataFrame(geo.tolist(), columns = geo_cols)

    # 기존 정보와 통합
    result = pd.concat([df.reset_index(drop=True), 
                        geo_df.reset_index(drop=True)], axis=1)
    result.to_excel(rstFileName, index=False)

    return result      


def searchMap(search_text):
    
    '''
    
    본 API에 대한 설명은 아래의 주소 참조
    https://apidocs.ncloud.com/ko/ai-naver/maps_geocoding/geocode/
    월 3,000,000회까지 무료
    
    https://apidocs.ncloud.com/ko/ai-naver/maps_geocoding/
    
    Return example
    -------------
    
    {
        "status": "OK",
        "meta": {
            "totalCount": 1,
            "page": 1,
            "count": 1
        },
        "addresses": [
            {
                "roadAddress": "경기도 성남시 분당구 불정로 6 그린팩토리",
                "jibunAddress": "경기도 성남시 분당구 정자동 178-1 그린팩토리",
                "englishAddress": "6, Buljeong-ro, Bundang-gu, Seongnam-si, Gyeonggi-do, Republic of Korea",
                "addressElements": [
                    {
                        "types": [
                            "POSTAL_CODE"
                        ],
                        "longName": "13561",
                        "shortName": "",
                        "code": ""
                    }
                ],
                "x": "127.10522081658463",
                "y": "37.35951219616309",
                "distance": 20.925857741585514
            }
        ],
        "errorMessage": ""
    }
    
    결과 항목
    --------
    도로명	지번	영문	광역	기초	읍면동	리	도로명	
    건물번호	건물명	번지	우편번호	 경도 위도

    
    '''
    from configparser import ConfigParser
    
    parser = ConfigParser()
    parser.read('config.ini')
    
    # naver geocoding을 사용할 때 필요한 정보 로딩
    client_id = parser.get('naver_id', 'client_id')
    client_secret = parser.get('naver_id', 'client_secret')

    encText = urllib.parse.quote(search_text)
    url = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode?query='+encText
    # url = "https://openapi.naver.com/v1/map/geocode.xml?query=" + encText # xml 결과
    request = urllib.request.Request(url)
    request.add_header("X-NCP-APIGW-API-KEY-ID",client_id)
    request.add_header("X-NCP-APIGW-API-KEY",client_secret)
    response = urllib.request.urlopen(request)
    rescode = response.getcode()
    if(rescode==200):
        response_body = response.read()
        mystr = response_body.decode('utf-8')
#        print(mystr)
#        mylst = [mystr['addresses']
        mystr = eval(mystr)
        
        # 검색 결과가 1개인 경우
        if mystr['meta']['totalCount'] == 1:
            
            mystr1 = []
            mystr1.append(mystr['addresses'][0]['roadAddress'])
            mystr1.append(mystr['addresses'][0]['jibunAddress'])
            mystr1.append(mystr['addresses'][0]['englishAddress'])
            mystr1.append(mystr['addresses'][0]['addressElements'][0]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][1]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][2]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][3]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][4]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][5]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][6]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][7]['longName'])
            mystr1.append(mystr['addresses'][0]['addressElements'][8]['longName'])
            mystr1.append(mystr['addresses'][0]['x'])
            mystr1.append(mystr['addresses'][0]['y'])
    
            return mystr1
        
        else:
            return mystr
    
#        # false, true를 대문자로 replace를 해주고,
#        mystr = mystr.replace('true',"True")
#        mystr = mystr.replace('false',"False")
#        
#        # string -> json 타입으로 바꾸자
#        mydic = eval(mystr)
#        
#        # 차례대로 끼워맞추다 보면 아래의 값으로 출력 할 수 있다.
#        print(mydic['result']['items'][0]['point']['y'])
#        print(mydic['result']['items'][0]['point']['x'])
    else:
        print("Error Code:" + rescode)    
        
        
def geoCheck(fileName, addr_ori_col, addr_converted_col, jibun_addr_col,
             comp_addr_col, chk_col):
    
    '''
    주소 검색이 실패한 경우 원 데이터의 주소 정보 또는 지번 주소를 
    검색용 주소로 저장하고 주소 변환이 실패함을 기록

    '''
    
    if doesFileExist(fileName):
            
        # 데이터 로딩
        data = pd.read_excel(fileName)
    
        # 도로명 주소 검색이 실패한 경우를 확인
        mask1 = data[addr_converted_col]=='status'
        mask2 = data[addr_converted_col].isnull()
        mask = mask1 | mask2
        mask_inv = ~mask
        
        idx1 = data[mask1].index
        idx2 = data[mask2].index
        idx = data[mask].index
        idx_inv = data[mask_inv].index
    
        # 주소 검색이 실패함을 기재
        data.loc[idx, chk_col] = 'fail'
        
        # 검색용 주소 comp_addr_col 생성
    
        # idx1 : 주소 검색 결과 = status
        # 이전 정보를 복사
        oldAddr = data.loc[idx1, addr_ori_col]
        data.loc[idx1, comp_addr_col] = oldAddr
        
        # idx2 : 도로명 주소가 존재하지 않는 경우, 지번 주소 저장
        jibunAddr = data.loc[idx2, jibun_addr_col]
        data.loc[idx2, comp_addr_col] = jibunAddr

        # idx_inv : 도로명 주소 복사
        roadAddr = data.loc[idx_inv, addr_converted_col]
        data.loc[idx_inv, comp_addr_col] = roadAddr
        
        # 새로운 파일 이름으로 저장
        newFileName = ''.join(fileName.split('.')[:-1])+'_chk.'+\
            fileName.split('.')[-1]
        
        data.to_excel(newFileName, index=False)
        
        return data   



# 길이가 길어서 리스트 형태로 정의하고, 추후 합침
patList = ['서울특별시|서울|부산광역시|부산|대구광역시|대구', 
           '인천광역시|인천|광주광역시|광주|대전광역시|대전',
           '울산광역시|울산|세종특별자치시|세종|경기도|경기',
           '강원도|강원|충청북도|충북|충청남도|충남|전라북도|전북',
           '전라남도|전남|경상북도|경북|경상남도|경남',
           '제주특별자치도|제주']

pat = '|'.join(patList)
reSIDO = re.compile(pat)

# 사업장명 전처리
irrelevant_regex = re.compile(r'[^가-힣\s0-9a-zA-Z]')
multispace_regex = re.compile(r'\s\s+')
stock_word_regex = re.compile(r'\(주\)|\(의\)|\(유\)|\(취\)|\(학\)|\(재\)|주\)|주식회사')




# 광역시도 축약형 설정
sido_dict = {'서울특별시':'서울', 
             '부산광역시':'부산',
             '대구광역시':'대구',
             '인천광역시':'인천',
             '광주광역시':'광주',
             '대전광역시':'대전',
             '울산광역시':'울산',
             '세종특별자치시':'세종',
             '경기도':'경기',
             '강원도':'강원', 
             '충청북도':'충북',
             '충청남도':'충남',
             '전라북도':'전북',
             '전라남도':'전남',
             '경상북도':'경북',
             '경상남도':'경남',
             '제주특별자치도':'제주'}

def extract_sido(string):
    '''
    문장에 포함된 광역시도 값을 추출
    '''

    global reSIDO

    matchObj = reSIDO.match(str(string).strip())
    if matchObj != None:
        result = matchObj.group()
        return result
    else:
        return None
    
    
def assign_no_symbols_name(df, col):
    '''
    df의 col값에 포함된 특수문자, 공백, 주식회사 등의 문자 제거
    
    '''
    
    return df.assign(
        newName=df[col]
             .str.replace(stock_word_regex, ' ')
             .str.replace(irrelevant_regex, ' ')
             .str.replace(multispace_regex, ' ')
             .str.strip()
             )


def modifyIndex(data, idx_col):
    '''
    비교 대상 파일의 idx_col이 중복인 경우 중복되지 않게 변경
    idx_col은 현재 index로 지정되지 않은 상태임을 가정

    '''
    
    data_ = data.copy()
    # idx_col의 개수 확인
    data_grp = data_.groupby(idx_col)[idx_col].count()
    
    # 중복 데이터 확인
    duplicatedIndex = data_grp[data_grp>1].index.tolist()
    
    # 중복데이터에 대해 인덱스 post_fix 추가 및 변경
    mask = data_[idx_col].isin(duplicatedIndex)
    newIndex = [idx+'_'+str(i) for i, idx, in enumerate(data_.loc[mask, idx_col])]
    data_.loc[mask, idx_col] = newIndex

    return data_

def getCustNoInfoFromEDSM():
    '''
    EDSM에 정보제공동의를 완료한 고객번호에 대한 정보를 받는 기능
    return값은 없으며, CustNoAPIRst.xlsx를 저장함
   
    '''
    df_list = []
    api_key = Constants.EDSM_API_KEY

    # API 호출    
    url =f'https://opm.kepco.co.kr:11080/OpenAPI/getCustNoList.do?serviceKey={api_key}&returnType=02'
    req = requests.get(url)
    html = req.text
    
    now = time.localtime()
    now_str = f'{now.tm_year:04d}{now.tm_mon:02d}{now.tm_mday:02d}_{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}'
    
    #JSON이 반환되지 않을 경우 오류 처리        
    try:
        
        html = html.replace('\\u0000', '')
        json_data = json.loads(html)
        df_tmp = pd.DataFrame(json_data['custNoInfoList'])
    except:
        print('에러 발생')

    df_tmp.to_excel(f'EDSM_CustNo_API_{now_str}.xlsx')
    print('getting API done')

class ElecData:
    
    '''
    21-02-15
    
    데이터 구분을 'postGres' -> 'post'로 수
    
    21-02-02 주석 수정
    hive 데이터
    
    tb_opm_001 테이블을 이용하여 사업장을 매칭한 후에
    사업장 정보 tb_a01_018 테이블을 이용하여 
    기본 설정(신고정보, 고객동의현황) 로드 후 '시도','부문', '업종' 정보를 추가
    
    postGres 데이터
    
    추후 설명 추가
  
    
    '''
    
    
    def __init__(self, fileName, period, dataCat = 'post', 
                 match = True,  # 매칭
                 select = True, busiDiv = None, # 자료 선택
                 category = None, enteCode = None, 
                 diff = False # 비교
                 ):
        
        self.fileName = fileName
        self.period = period
        self.busiDiv = busiDiv
        self.category = category
        self.enteCode = enteCode

        self.srcPath = os.getcwd()
        self.dataPath = f'{self.srcPath}\\..\\data'    

        self.jijung = self.load_jijung()
        self.tb_opm_001 = self.load_tb_opm_001()
        self.tb_a01_018 = self.load_tb_a01_018()
        # self.edsm_custNo_api = self.load_edsm_custNo_api()
        # self.edsm_custNo_excel= self.load_edsm_custNo_excel()

        self.diff = diff
        
        # postGres 매칭 결과, EDSM 파일 비교, 필요한 경우만 수행
        if diff:
            self.compEdsm = self.compare_edsm()

        # 파일의 이름을 보고 hive, postGres 여부 판단
        # 파일 이름을 지을 때 유의하여 작성 필요
        if 'hive' in fileName:
            dataCat = 'hive'
        elif 'post' in fileName:
            dataCat = 'post'

        self.dataCat = dataCat
        
        # 전력데이터 로드        
        if dataCat == 'post':
            self.data = self.load_postGres()
        elif dataCat == 'hive':
            self.data = self.load_hive()
        
        
        # 전력데이터 매칭
        if match == True:
            
            # ente_code 추가
            self.match_data_tb_opm_001()
            
            # 부문, 업종/용도 추가
            self.match_jijung()

            # 부문, 업종/용도 추가
            self.match_tb_a01_018()

        if select == True:
            
            # 기간 선택
            if dataCat =='post':
                self.data = self.select_period(date_col='meter_dd', 
                                               date_format='%Y%m%d')
            
            elif dataCat =='hive':
                self.data = self.select_period(date_col='part_key_mr_ymd', 
                                               date_format='%Y%m%d')
            # 부문, 업종/용도로 데이터 선택
            
            # 특정 사업장을 선택한다면
            if enteCode:
                self.data = self.select_enteCode()
            
            # 용도 값이 설정되어 있다면
            elif category:
                self.data = self.select_category()
           
            # 부문이 설정되어 있다면
            elif busiDiv:
                self.data = self.select_busiDiv()
                
    def load_tb_a01_018(self, fileName = Constants.TB_A01_018):

        dtype = {'ENTE_CODE':str,'KEMC_OLDX_CODE':str, 
                 'CITY_PROV_CODE':str}
        usecols = Constants.TB_A01_018_USE_COLS

        print('TB_A01_018(신고 업체 정보) 데이터 로드')                
        data = load_pickle_first(self.dataPath, fileName, 
                                 reload = Constants.RELOAD,
                                 # reload = True, 
                                 pickleWrite=True, dtype = dtype, usecols = usecols)
        
        sido = data.CITY_PROV_CODE.map(Constants.SIDODict)
        data['시도'] = sido
        
        busiCat = data.KEMC_OLDX_CODE.map(Constants.busiCatDict)
        data['업종'] = busiCat

        divCat = data.KEMC_OLDX_CODE.map(Constants.divCatDict)
        data['부문'] = divCat
        
        return data
    
    def select_enteCode(self):

        # postGres인 경우에는 전처리를 거친 ente_code 사용
        if self.dataCat =='post':
            mask = self.data['ente'].isin(self.enteCode)
        
        # hive인 경우에는 tb_opm_001 ENTE_CODE 사용
        elif self.dataCat =='hive':
            mask = self.data['ENTE_CODE'].isin(self.enteCode)
            
        return self.data.loc[mask]


    def select_category(self):
        mask = self.data['업종'].isin(self.category)
        return self.data.loc[mask]
            
    def select_busiDiv(self):
        mask = self.data['구분'].isin(self.busiDiv)
        return self.data.loc[mask]
            

    def select_period(self, date_col, date_format):
        
        self.data[date_col] = pd.to_datetime(self.data[date_col], format=date_format)
        
        period_split = self.period.split(':')
        mask1 = self.data[date_col] >= period_split[0]
        mask2 = self.data[date_col] <= period_split[1]
        mask = mask1 & mask2
        
        return self.data.loc[mask]        

    def match_jijung(self):

        # 필요한 정보만 추출
        jijung = self.jijung[['ENTE', 'YEAR']]

        if self.dataCat == 'post':
            self.data = pd.merge(self.data, jijung, left_on = 'ente',
                             right_on='ENTE', how='left')
        
        # hive는 ente_code가 매칭되어 있지 않으므로 ENTE_CODE 이용
        # hive와 postGres가 결과가 차이가 있음을 인지해야 함
        
        elif self.dataCat == 'hive':
            self.data = pd.merge(self.data, jijung, left_on = 'ENTE_CODE',
                             right_on='ENTE', how='left')


    def match_tb_a01_018(self):

        # 필요한 정보만 추출        
        tb_a01_018 = self.tb_a01_018[['ENTE_CODE', '시도','부문', '업종']]

        if self.dataCat == 'post':
            self.data = pd.merge(self.data, tb_a01_018, left_on = 'ente',
                             right_on='ENTE_CODE', how='left')
        
        # hive는 ente_code가 매칭되어 있지 않으므로 ENTE_CODE 이용
        # hive와 postGres가 결과가 차이가 있음을 인지해야 함
        
        elif self.dataCat == 'hive':
            self.data = pd.merge(self.data, tb_a01_018, left_on = 'ENTE_CODE',
                             right_on='ENTE_CODE', how='left')
            

    def add_ente_info(self):
        pass

    def match_data_tb_opm_001(self):
        '''
        신고 tb_opm_001의 ente_code, 한전고객번호 관계를 이용하여 
        한전 전력데이터에 ente_code, 부문, 업종/용도 등을 매칭
        
        '''

        # 한전 데이터를 신고 tb_opm_001을 이용하여 매칭을 할 때는
        # 신고 tb_opm_001을 먼저 전처리해야 하나 여기서는 생략
        # 전처리를 진행하지 않으므로 매칭되야 하는 사업장이 일부 누락됨
        # 추후 시간이 허용되면 전처리 로직 추가
        ente = self.tb_opm_001.loc[:,['CUST_NO', 'ENTE_CODE']]
        self.data = pd.merge(self.data, ente, left_on='cust_no',
                             right_on='CUST_NO', how='left')

        mask_matched = self.data.ENTE_CODE.notnull()
        matched = self.data.loc[mask_matched , ['cust_no', 'meter_no', 'ENTE_CODE']].nunique()
        matched.rename('matched', inplace=True)

        mask_unmatched = self.data.ENTE_CODE.isnull()
        unmatched = self.data.loc[mask_unmatched, ['cust_no', 'meter_no']].nunique()
        unmatched.rename('unmatched', inplace=True)
        
        total = pd.concat([matched, unmatched], axis=1)

        # postGres는 매칭이 정확히 되었는지 확인하는 용도로 추가 비교
        if (self.dataCat == 'post') and (self.diff == True):
            
            mask_diff = self.data['ente']!=self.data['ENTE_CODE']
            self.diff = self.data.loc[mask_diff]
            self.diff.to_excel('diff.xlsx')

        self.status = total
            
    def compare_edsm(self):
        
        '''
        2개 자료의 고객번호 개수를 비교
        
        '''
        grp_excel = self.edsm_custNo_excel.groupby('고객번호')['고객명'].count()
        grp_excel.rename('excel', inplace=True)
        grp_api = self.edsm_custNo_api.groupby('custNo')['custNm'].count()
        grp_api.rename('api', inplace=True)
        grp_total = pd.concat([grp_excel, grp_api], axis=1)
        grp_total.to_excel('grp_total.xlsx')
        if grp_total.isnull().sum().sum():
            print('Check EDSM data!')
        
        return grp_total   
            
    def load_edsm_custNo_api(self, fileName = Constants.EDSM_CUSTNO_API):
        
        dtype = {'custNo':str}
        print('EDSM_CustNo_API(한전 EDSM 고객번호 API) 로드')                
        return load_pickle_first(self.dataPath, fileName, 
                                 reload = Constants.RELOAD,
                                 # reload = True, 
                                 pickleWrite=True, dtype = dtype)
        
            
            
    def load_tb_opm_001(self, fileName = Constants.TB_OPM_001):

        dtype = {'ENTE_CODE':str,'CUST_NO':str}
        usecols = Constants.TB_OPM_001_USE_COLS

        print('tb_opm_001(신고 한전 고객번호) 데이터 로드')                
        return load_pickle_first(self.dataPath, fileName, 
                                 reload = Constants.RELOAD,
                                 # reload = True, 
                                 pickleWrite=True, dtype = dtype, usecols = usecols)
        
        
    def load_postGres(self):
        
        dtype = {'ente':str,'cust_no':str, 'meter_no':str, 
                 'kemc_oldx_code':str}
        
        # 전력데이터 외 다른 데이터를 사용코자 할 때는 수정
        # usecols = Constants.POSTGRES_USE_COLS

        print('postGres 데이터 로드')                
        data = load_pickle_first(self.dataPath, 
                                 self.fileName, 
                                 reload = Constants.RELOAD,
                                 # reload = True, 
                                 pickleWrite=True, dtype = dtype)
        
        return data
        
    def load_hive(self):

        dtype = {'tb_day_lp_30day_bfor_data.cust_no':str,
                 'tb_day_lp_30day_bfor_data.meter_no':str}
    
        print('hive 데이터 로드')                

        data = load_pickle_first(self.dataPath, 
                                 self.fileName, 
                                  reload = Constants.RELOAD,
                                 # reload = True, 
                                 pickleWrite=True, dtype = dtype)
        # columns 이름 정리
        new_columns = [col.replace(Constants.HIVE_ELEC_TABLE,'') for col in data.columns]
        data.columns = new_columns

        # 불필요한 컬럼 삭제            
        if 'Unnamed: 0' in data.columns:
            data.drop('Unnamed: 0', axis=1, inplace=True)
        
        return data
        
    def load_edsm_custNo_excel(self, fileName = Constants.EDSM_AGREEMENT):

        print('custNo(한전 EDSM 고객동의현황 excel) 로드')
        dtype = {'고객번호':str}
        return load_pickle_first(self.dataPath, fileName, 
                                  reload = Constants.RELOAD,
                                 # reload = True,
                                 pickleWrite=True, skiprows=1, dtype = dtype)
        
        
    def load_jijung(self, fileName = Constants.JIJUNG):
        '''
        jijung table에는 업체명, 부문, 업종, 기업규모 등의 정보가 있음
        jijung table은 전력데이터를 수집 시작한 2019년 7월 이후에 
        생성된 jijung table을 통합해서 사용해야 누락을 방지할 수 있음
        
        '''
        dtype = {'ENTE':str}
        usecols = Constants.JIJUNG_USE_COLS
        
        print('jijung 로드')
        return load_pickle_first(self.dataPath, fileName, 
                                  reload = Constants.RELOAD,
                                 # reload = True,
                                 pickleWrite=True, usecols = usecols, dtype=dtype)


def timer(func):
    
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        total = time.time() - start
        print('소요시간 {:.1f}s\n'.format(total))
        return result
    return wrapper


@timer
def load_pickle_first(myDir, fileName, reload = False, pickleWrite=True, **kwargs):
    
    '''
    pickle 파일이 존재하면 pickle 파일을 로드
    pickle 파일이 존재하지 않으면 pickle 파일을 로드하고 pickle 파일 저장
    
    '''

    exists, path = chkFileExists(myDir, fileName)
    
    if exists:
    
        # s()
        fileName_pkl = ''.join(fileName.split('.')[:-1])+'.pkl'
        extension = fileName.split('.')[-1]
    
        if reload:
            
            # 파일의 존재여부는 이전에 확인한 것으로 재확인 불필요   
            data = load_csv_excel_file(path, fileName, **kwargs)
    
            # 다음 번 로딩을 위해 pickle 파일 저장
            if pickleWrite:
                data.to_pickle(os.path.join(path, fileName_pkl))
        
        else:
        
            # 먼저 pickle 파일 로드
            try:
                data = pd.read_pickle(os.path.join(path, fileName_pkl))
            
            except:
                data = load_csv_excel_file(path, fileName, **kwargs)
    
                # 다음 번 로딩을 위해 pickle 파일 저장
                if pickleWrite:
                    data.to_pickle(os.path.join(path, fileName_pkl))    
        
        if 'usecols' in kwargs:
            data = data[kwargs['usecols']]
        
        return data
    
    else:
        raise FileNotFoundError('check if {fileName} exists')


def load_csv_excel_file(path, fileName, **kwargs):
    
    '''
    excel, csv 파일 로드
    
    '''
    
    # 파일 확장자 확인
    extension = fileName.split('.')[-1]
    print(f'(csv 또는 excel 파일 로드)')
    
    try:
        
        # pdb.set_trace()
        if (extension == 'xlsx') or (extension == 'xls'):
            data = pd.read_excel(os.path.join(path, fileName), **kwargs)
    
        elif (extension == 'csv'):
            data = pd.read_csv(os.path.join(path, fileName), low_memory=False, **kwargs)  
        
        return data
    
    # 파일이 없는 경우 
    except FileNotFoundError:
        
        raise FileNotFoundError
        
        

def chkFileExists(myDir, fileName):
    '''
    하위 폴더까지 검색해서 파일이 존재하는 경우 경로 반환
    '''
    
    
    for root, dirs, files in os.walk(myDir, topdown=False):
        if fileName in files:
            
            return True, root
    
    return False, ''



class Error(Exception):
    """Base class for exceptions in this module."""
    pass


def melt_elec(df, id_vars, value_vars):
    
    '''가로형 데이터를 세로형으로 전환'''
    
    df_melt = df.melt(id_vars = id_vars, value_vars=value_vars)
    
    date = pd.to_datetime(df_melt['meter_dd'], format='%Y-%m-%d')
    hour = df_melt['variable'].str[-4:-2].astype(float) * pd.Timedelta('1h') 
    minute = df_melt['variable'].str[-2:].astype(float) * pd.Timedelta('1m')
    new_date = date + hour + minute
    
    df_melt[id_vars] = new_date
    
    # 불필요 컬럼 삭제
    df_melt = df_melt.set_index('meter_dd')
    df_melt = df_melt.drop('variable', axis=1)
    return df_melt


def isRange(strDate):
    '''
    날짜 구간으로 지정되어 있는지 여부 확인
    
    '''
    
    # 날짜가 구간으로 지정되어 있다면
    if len(strDate.split(':'))==2:
        date1 = strDate.split(':')[0]
        date2 = strDate.split(':')[1]
        return True, date1, date2
    
    elif len(strDate.split(':'))==1:
        return False, strDate


def viewHeader(myDir, fileName, reload = False, pickleWrite=False, **kwargs):
    
    '''
    대용량 파일을 로드하기 전에 파일의 header를 확인
    
    '''

    exists, path = chkFileExists(myDir, fileName)
    
    if exists:
    
        # s()
        fileName_pkl = ''.join(fileName.split('.')[:-1])+'.pkl'
        extension = fileName.split('.')[-1]
    
        if reload:
            
            # 파일의 존재여부는 이전에 확인한 것으로 재확인 불필요   
            data = load_csv_excel_file(path, fileName, **kwargs)
    
            # 다음 번 로딩을 위해 pickle 파일 저장
            if pickleWrite:
                data.to_pickle(os.path.join(path, fileName_pkl))
        
        else:
        
            # 먼저 pickle 파일 로드
            try:
                data = pd.read_pickle(os.path.join(path, fileName_pkl))
                # 만약 nrows를 설정하였다면 일부만 데이터를 반
                if 'nrows' in kwargs:
                    data = data.iloc[:kwargs['nrows']]
            
            except:
                data = load_csv_excel_file(path, fileName, **kwargs)
    
                # 다음 번 로딩을 위해 pickle 파일 저장
                if pickleWrite:
                    data.to_pickle(os.path.join(path, fileName_pkl))    
        
        if 'usecols' in kwargs:
            data = data[kwargs['usecols']]
        
        return data
    
    else:
        raise FileNotFoundError('check if {fileName} exists')


def grpEnteLoadRate(df, data_type = 'post', ente_col = 'ente',
                cust_no_col = 'cust_no', date_col='meter_dd',
                elec_kwd = 'elcp'):
    
    '''
    postGres의 데이터를 업체코드별, 고객번호별 정리
    1)전력사용량 합계
    2)전력데이터 기간
    
    
    test
    ------
    
    df = data.data.copy()    
    ente_col = 'ente'
    cust_no_col = 'cust_no'
    date_col='meter_dd'
    elec_kwd = 'elcp'
    
    '''
    
    # postGres 데이터인 경우
    if data_type == 'post':
        
        
        # 전력사용량 컬럼 선택
        elec_col = [col for col in df.columns if elec_kwd in col]
        
        # 업체별, 고객번호별, 날짜별 전력사용량 합산
        # 계측기가 여러 개 존재할 수 있으므로 고객번호로 먼저 통합 필요
        # 고객번호로 먼저 통합하지 않으면 일자별 unstack시 오류가 발생함
        grp1 = df.groupby([ente_col, cust_no_col, date_col])\
            [elec_col].sum()
            
        # 일자별로 합계, 평균, 최대값 계산
        grp1['elecSum'] = grp1[elec_col].sum(axis=1)
        grp1['elecAvg'] = grp1[elec_col].mean(axis=1)
        grp1['elecMax'] = grp1[elec_col].max(axis=1)
        grp2 = grp1[['elecSum', 'elecAvg', 'elecMax']].unstack()

        elecAvg = grp2.loc[:, ('elecAvg', slice(None))].values
        elecMax = grp2.loc[:, ('elecMax', slice(None))].values
        # elecMaxYear = elecMax.max(axis=1)
        elecMaxYear = np.nanmax(elecMax, axis=1)
        elecMaxYear = elecMaxYear.reshape(-1, 1)
        
        # 전력사용량 정보 추출
        cust_no_elecCons = grp2.loc[:,('elecSum', slice(None))]

        # test
        # grp2_sel = grp2.loc[(slice(None), '0135429711'), :]
        # grp2_sel.to_excel('grp2_sel.xlsx')
        

        # index 번호 확인        
        # for i, idx in enumerate(list(grp2.index.get_level_values(1))):
        #     if idx == '0135429711':
        #         print(i)
                
                    
        ## 부하율 계산        
        # 날짜 정보 추출, 평균 전력에서 2번째 수준의 컬럼 정보 추출
        dates = grp2.loc[:,('elecAvg', slice(None))].columns.get_level_values(1)
        
        # 멀티인덱스 설정
        idx_lst = [['loadRate'], dates]
        idx_load = pd.MultiIndex.from_product(idx_lst, names = ['l1', 'l2'])
        
        # 분자가 0이거나 nan인 경우는 제외하고 부하율 계산
        elecDayLoadRate = np.divide(elecAvg, elecMax, 
                                    where=(~np.isnan(elecMax))&(elecMax!=0))
        
        elecDayLoadRate_byYearMax = np.divide(elecAvg, elecMaxYear, 
                                    where=(~np.isnan(elecMaxYear))&(elecMaxYear!=0))

        # 고객번호 단위의 일 부하율 계산
        
        cust_no_load_by_dailyMax = pd.DataFrame(elecDayLoadRate, index=grp2.index, 
                                columns = idx_load)

        cust_no_load_by_yearlyMax = pd.DataFrame(elecDayLoadRate_byYearMax, index=grp2.index, 
                                columns = idx_load)
        
        # cust_no_load_by_yearlyMax.to_excel('cust_no_load_by_yearlyMax.xlsx')
        # grp_load.to_excel('grp_load.xlsx')
        
        # cust_no = '0135429711'
       
        # 사용량, 부하율(일최대), 부하율(기간최대)
        return cust_no_elecCons, cust_no_load_by_dailyMax, cust_no_load_by_yearlyMax


def load_json(path, fileName):
    
    '''
    
    json 파일을 읽어서 dataFrame으로 저장
    
    test
    -----
    
    path = 'D:\데이터 분석\data'
    fileName = 'custLst_210216.txt'
    
    '''
    
    exists, path = chkFileExists(path, fileName)
    
    # 파일이 존재하는 경우
    if exists:
        with open(os.path.join(path, fileName), 'r', encoding='utf-8') as f:
            json_str = f.read()
            
        import json
        json_data = json.loads(json_str)
        df = pd.DataFrame(json_data['custNoInfoList'])
        return df
    else:
        print('No files')


def load_cust_no_list_hive(path, fileName, date):
    '''
    json 파일을 읽어서 dataFrame으로 저장

    test
    -----
    
    path = 'D:\데이터 분석\data'
    fileName = 'hive_tb_cust_no_list_20201201_20210217.csv'
    
    '''

    exists, path = chkFileExists(path, fileName)
    
    # 파일이 존재하는 경우
    if exists:
        df = pd.read_csv(os.path.join(path, fileName), dtype={'tb_cust_no_list.cust_no':str})
        hive_table = 'tb_cust_no_list.'
        # s()
        
        df.columns = df.columns.str.replace(hive_table, '')
        df.cust_no = df.cust_no.str.zfill(10)
        df['writ_dtm'] = pd.to_datetime(df['writ_dtm'], format='%Y-%m-%d %H:%M:%S')
        df['writ_dtm'] = df['writ_dtm'].apply(lambda x:datetime.datetime.strftime(x, format='%Y%m%d'))
        
        mask = df['writ_dtm'] == date
        
        return df[mask]
    else:
        print('No files')

    
    
    


if __name__ == '__main__':

    getCustNoInfoFromEDSM()
    
    # pd.set_option('display.max_columns', 500)
    # pd.set_option('display.max_rows', 500)
    
    # small change
    
