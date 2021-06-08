from typing import no_type_check_decorator
import pandas as pd
from datetime import datetime, date,  timedelta
from dateutil.relativedelta import relativedelta
from Credential import *
from math import radians, cos, sin, asin, sqrt
import numpy as np
import psycopg2
import pyodbc

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Food\\Integrate_external_data\\'
file_name='test_data.xlsx'

def Read_FB_Population_General_Prv(prv_input, d_input, s_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        ## Not working coz in sub district level one will only get information within 5 km, beyond that will need data across district
        # if(len(s_input)>0):    
        #         print(' Sub district **************************************** ')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where t_name_t = '"""+str(s_input)+"""'  """
        # elif(len(d_input)>0):                
        #         print(' District +++++++++++++++++++++++++++++++++++++++++++++++')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where a_name_t = '"""+str(d_input)+"""'  """
        # elif(len(prv_input)>0):
        #         print(' Provicne ------------------------------------------------- ')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where p_name_t = '"""+str(prv_input)+"""'  """
        # else:
        #         print( ' ALL =================================================== ')
        #         sql = """SELECT * FROM public.\"fb_population_general\" """

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_general\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_general\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_711_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_711\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_711\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Retail_Shop_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_retailshop\" where p_name_t = '"""+str(prv_input)+"""' and type_ in ('Convenience store','CP','Family Mart','Lawson 108','Freshmart','108 Shop') """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_retailshop\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Residential_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_residential\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_residential\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Restaurant_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_restaurant\" where p_name_t = '"""+str(prv_input)+"""' and left(goodfors,4) in ('จานด','เดลิ') """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_restaurant\" where left(goodfors,4) in ('จานด','เดลิ')  """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Read_Ext_Education_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_education\" where p_name_t = '"""+str(prv_input)+"""' and cate in ('มหาวิทยาลัย') """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_education\" where cate in ('มหาวิทยาลัย')  """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Read_Ext_Hotel_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"ext_hotel\" where p_name_t = '"""+str(prv_input)+"""' """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"ext_hotel\"   """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def ComputePopulation(dfIn):
        mainDf=pd.DataFrame(columns=['name','lat','lng','p_name_t', 'a_name_t', 't_name_t','pop5k','pop20k'])
        for n in range(len(dfIn)):
                store_province=dfIn['p_name_t'].iloc[n]
                store_district=dfIn['a_name_t'].iloc[n]
                store_subdistrict=dfIn['t_name_t'].iloc[n]
                store_name=dfIn['Name'].iloc[n]
                s_lat=dfIn['lat'].iloc[n]
                s_lng=dfIn['lng'].iloc[n]
                #print(' name : ',store_name,'  ::  ',store_province, ' --- ',s_lat,' : ',s_lng)
                dfPop=Read_FB_Population_General_Prv(store_province, store_district, store_subdistrict)
                #dfPop.to_csv(file_path+'check.csv')        
                #dfPop=pd.read_excel(file_path+'check_pop.xlsx')
                #print(len(dfPop),' --  Pop --- > ',dfPop.head(10))
                dfDummy=dfPop.copy()  #.head(10)
                #print(len(dfDummy),' ----  ',dfDummy)
                sum_pop_5km=0
                sum_pop_20km=0
                for m in range(len(dfDummy)):
                        p_name=dfDummy['p_name_t'].iloc[m]
                        p_lat=dfDummy['lat'].iloc[m]
                        p_lng=dfDummy['lng'].iloc[m]
                        p_pop=dfDummy['population'].iloc[m]
                        #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng,' --> ',p_pop)
                        a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                        #print('Distance (km) : ', a)
                        radius5=5.0
                        radius20=20.0
                        if a <= radius5:
                                print('Inside the area ',radius5, '  km')
                                sum_pop_5km=sum_pop_5km+p_pop                        
                        elif( a>radius5 and  a <= radius20):
                                print('Inside the area ',radius20,'  km')
                                sum_pop_20km=sum_pop_20km+p_pop                        
                        else:                        
                                #print('Outside the area')                
                                continue
                mainDf=mainDf.append({'name':store_name,'lat':s_lat, 'lng':s_lng,'p_name_t':store_province, 'a_name_t':store_district, 't_name_t':store_subdistrict  ,'pop5k':sum_pop_5km, 'pop20k':sum_pop_20km},ignore_index=True)
                print(' main : ',mainDf)

        print(' ====================================================== ')
        print(' main : ',mainDf)

        del  dfPop, dfDummy
        del store_province, store_name, s_lat, s_lng, sum_pop_5km, sum_pop_20km, p_name, p_lat, p_lng, p_pop
        return mainDf
def ComputePopulation_1km_rev2(store_province, store_district, store_subdistrict, s_lat, s_lng):
        dfPop=Read_FB_Population_General_Prv(store_province, store_district, store_subdistrict)
        #dfPop.to_csv(file_path+'check.csv')        
        #dfPop=pd.read_excel(file_path+'check_pop.xlsx')
        #print(len(dfPop),' --  Pop --- > ',dfPop.head(10))
        dfDummy=dfPop.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_pop_5km=0
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]
                p_pop=dfDummy['population'].iloc[m]
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng,' --> ',p_pop)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius5=1.0                
                if a <= radius5:
                        print('Population : Inside the area ',str(radius5),'  km')
                        sum_pop_5km=sum_pop_5km+p_pop 
                else:                        
                        #print('Outside the area')                
                        continue

        return sum_pop_5km
def ComputePopulation_5km_rev2(store_province, store_district, store_subdistrict, s_lat, s_lng):
        dfPop=Read_FB_Population_General_Prv(store_province, store_district, store_subdistrict)
        #dfPop.to_csv(file_path+'check.csv')        
        #dfPop=pd.read_excel(file_path+'check_pop.xlsx')
        #print(len(dfPop),' --  Pop --- > ',dfPop.head(10))
        dfDummy=dfPop.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_pop_20km=0
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]
                p_pop=dfDummy['population'].iloc[m]
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng,' --> ',p_pop)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=5.0                
                if a <= radius:
                        print('Population : Inside the area ',str(radius),'  km')
                        sum_pop_20km=sum_pop_20km+p_pop 
                else:                        
                        #print('Outside the area')                
                        continue

        return sum_pop_20km

def Get711Store(dfIn):
        mainDf=pd.DataFrame(columns=['name','lat','lng','p_name_t', 'store_711'])
        for n in range(len(dfIn)):
                store_province=dfIn['p_name_t'].iloc[n]
                store_district=dfIn['a_name_t'].iloc[n]
                store_subdistrict=dfIn['t_name_t'].iloc[n]
                store_name=dfIn['Name'].iloc[n]
                s_lat=dfIn['lat'].iloc[n]
                s_lng=dfIn['lng'].iloc[n]
                print(' name : ',store_name,'  ::  ',store_province, ' --- ',s_lat,' : ',s_lng)        
                df711=Read_Ext_711_Prv(store_province)
                print(len(df711),' ============= ',df711.head(10))
                dfDummy=df711.copy()  #.head(10)
                print(len(dfDummy),' ----  ',dfDummy)
                sum_711_store=0        
                for m in range(len(dfDummy)):
                        p_name=dfDummy['p_name_t'].iloc[m]
                        p_lat=dfDummy['lat'].iloc[m]
                        p_lng=dfDummy['lng'].iloc[m]
                        p_code=dfDummy['code'].iloc[m]
                        print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng,' --> ',p_code)
                        a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                        #print('Distance (km) : ', a)
                        radius=1.0                
                        if a <= radius:
                                print('Inside the area 1 km')
                                sum_711_store=sum_711_store+1                        
                        else:                        
                                #print('Outside the area')                
                                continue
                mainDf=mainDf.append({'name':store_name,'lat':s_lat, 'lng':s_lng,'p_name_t':store_province,  'store_711':sum_711_store},ignore_index=True)
                print(' main : ',mainDf)

        del dfDummy
        del sum_711_store, p_name, p_lat, p_lng, p_code, store_name, store_province, store_district, store_subdistrict

        return mainDf
def Get711Store_rev2(store_province, s_lat, s_lng):
        df711=Read_Ext_711_Prv(store_province)
        #print(len(df711),' ============= ',df711.head(10))
        dfDummy=df711.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_711_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]
                p_code=dfDummy['code'].iloc[m]
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng,' --> ',p_code)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=1.0                
                if a <= radius:
                        print('711 : Inside the area ',str(radius),' km')
                        sum_711_store=sum_711_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue

        del df711, dfDummy
        del p_name, p_lat, p_lng
        return sum_711_store

def GetExtRetail(dfIn):
        mainDf=pd.DataFrame(columns=['name','lat','lng','p_name_t', 'store_retail'])
        for n in range(len(dfIn)):
                store_province=dfIn['p_name_t'].iloc[n]
                store_district=dfIn['a_name_t'].iloc[n]
                store_subdistrict=dfIn['t_name_t'].iloc[n]
                store_name=dfIn['Name'].iloc[n]
                s_lat=dfIn['lat'].iloc[n]
                s_lng=dfIn['lng'].iloc[n]
                print(' name : ',store_name,'  ::  ',store_province, ' --- ',s_lat,' : ',s_lng)        
                dfRs=Read_Ext_Retail_Shop_Prv(store_province)
                print(len(dfRs),' ============= ',dfRs.head(10))
                dfDummy=dfRs.copy()  #.head(10)
                print(len(dfDummy),' ----  ',dfDummy)
                sum_Rs_store=0        
                for m in range(len(dfDummy)):
                        p_name=dfDummy['p_name_t'].iloc[m]
                        p_lat=dfDummy['lat'].iloc[m]
                        p_lng=dfDummy['lng'].iloc[m]                      
                        print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                        a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                        #print('Distance (km) : ', a)
                        radius=1.0                
                        if a <= radius:
                                print('Inside the area ',str(radius), ' km')
                                sum_Rs_store=sum_Rs_store+1                        
                        else:                        
                                #print('Outside the area')                
                                continue
                mainDf=mainDf.append({'name':store_name,'lat':s_lat, 'lng':s_lng,'p_name_t':store_province,  'store_retail':sum_Rs_store},ignore_index=True)
                print(' main : ',mainDf)

        del dfDummy
        del sum_Rs_store, p_name, p_lat, p_lng, store_name, store_province, store_district, store_subdistrict

        return mainDf
def GetExtRetail_rev2(store_province, s_lat, s_lng):
        dfRs=Read_Ext_Retail_Shop_Prv(store_province)
        #print(len(dfRs),' ============= ',dfRs.head(10))
        dfDummy=dfRs.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_Rs_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]                      
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=1.0                
                if a <= radius:
                        print('Retail : Inside the area ',str(radius), ' km')
                        sum_Rs_store=sum_Rs_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue


        del dfRs, dfDummy
        del p_name, p_lat, p_lng
        return sum_Rs_store

def GetExtResidential(dfIn):
        mainDf=pd.DataFrame(columns=['name','lat','lng','p_name_t', 'residential'])
        for n in range(len(dfIn)):
                store_province=dfIn['p_name_t'].iloc[n]
                store_district=dfIn['a_name_t'].iloc[n]
                store_subdistrict=dfIn['t_name_t'].iloc[n]
                store_name=dfIn['Name'].iloc[n]
                s_lat=dfIn['lat'].iloc[n]
                s_lng=dfIn['lng'].iloc[n]
                print(' name : ',store_name,'  ::  ',store_province, ' --- ',s_lat,' : ',s_lng)        
                dfRs=Read_Ext_Residential_Prv(store_province)
                print(len(dfRs),' ============= ',dfRs.head(10))
                dfDummy=dfRs.copy()  #.head(10)
                print(len(dfDummy),' ----  ',dfDummy)
                sum_Rs_store=0        
                for m in range(len(dfDummy)):
                        p_name=dfDummy['p_name_t'].iloc[m]
                        p_lat=dfDummy['lat'].iloc[m]
                        p_lng=dfDummy['lng'].iloc[m]                      
                        print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                        a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                        #print('Distance (km) : ', a)
                        radius=5.0                
                        if a <= radius:
                                print('Inside the area ',str(radius),'  km')
                                sum_Rs_store=sum_Rs_store+1                        
                        else:                        
                                #print('Outside the area')                
                                continue
                mainDf=mainDf.append({'name':store_name,'lat':s_lat, 'lng':s_lng,'p_name_t':store_province,  'residential':sum_Rs_store},ignore_index=True)
                print(' main : ',mainDf)

        del dfDummy
        del sum_Rs_store, p_name, p_lat, p_lng, store_name, store_province, store_district, store_subdistrict

        return mainDf
def GetExtResidential_rev2(store_province, s_lat, s_lng):
        dfRs=Read_Ext_Residential_Prv(store_province)
        #print(len(dfRs),' ============= ',dfRs.head(10))
        dfDummy=dfRs.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_Rs_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]                      
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=5.0                
                if a <= radius:
                        print('Resident : Inside the area ',str(radius),'  km')
                        sum_Rs_store=sum_Rs_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue

        del dfRs, dfDummy
        del p_name, p_lat, p_lng

        return sum_Rs_store

def GetExtRestaurant_rev2(store_province, s_lat, s_lng):
        dfRs=Read_Ext_Restaurant_Prv(store_province)
        #print(len(dfRs),' ============= ',dfRs.head(10))
        dfDummy=dfRs.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_Rs_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]                      
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=1.0                
                if a <= radius:
                        print('Restaurant : Inside the area ',str(radius),'  km')
                        sum_Rs_store=sum_Rs_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue

        del dfRs, dfDummy
        del p_name, p_lat, p_lng

        return sum_Rs_store

def GetExtEducation_rev2(store_province, s_lat, s_lng):
        dfRs=Read_Ext_Education_Prv(store_province)
        #print(len(dfRs),' ============= ',dfRs.head(10))
        dfDummy=dfRs.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_Rs_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]                      
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=1.0                
                if a <= radius:
                        print('Education : Inside the area ',str(radius),'  km')
                        sum_Rs_store=sum_Rs_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue

        del dfRs, dfDummy
        del p_name, p_lat, p_lng

        return sum_Rs_store

def GetExtHotel_rev2(store_province, s_lat, s_lng):
        dfRs=Read_Ext_Hotel_Prv(store_province)
        #print(len(dfRs),' ============= ',dfRs.head(10))
        dfDummy=dfRs.copy()  #.head(10)
        #print(len(dfDummy),' ----  ',dfDummy)
        sum_Rs_store=0        
        for m in range(len(dfDummy)):
                p_name=dfDummy['p_name_t'].iloc[m]
                p_lat=dfDummy['lat'].iloc[m]
                p_lng=dfDummy['lng'].iloc[m]                      
                #print(m,' ==> ',p_name,' :: ',p_lat,' : ',p_lng)
                a = haversine(float(p_lng), float(p_lat), float(s_lng), float(s_lat))

                #print('Distance (km) : ', a)
                radius=1.0                
                if a <= radius:
                        print('Hotel : Inside the area ',str(radius),'  km')
                        sum_Rs_store=sum_Rs_store+1                        
                else:                        
                        #print('Outside the area')                
                        continue

        del dfRs, dfDummy
        del p_name, p_lat, p_lng

        return sum_Rs_store

##----------------------------------------------------------------------
## read test data
dfIn=pd.read_excel(file_path+file_name, sheet_name='Sheet1')
print(len(dfIn),' -- dfIn --- > ',dfIn.head(10))
###--------------------------------------------------------------------------

### Compute population around each store locations
dfIn['pop_1km']=dfIn.apply(lambda x: ComputePopulation_1km_rev2(x['p_name_t'],x['a_name_t'],x['t_name_t'],x['lat'],x['lng']),axis=1)
dfIn['pop_5km']=dfIn.apply(lambda x: ComputePopulation_5km_rev2(x['p_name_t'],x['a_name_t'],x['t_name_t'],x['lat'],x['lng']),axis=1)
### number 7-11 in 1km
dfIn['711_1km']=dfIn.apply(lambda x: Get711Store_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
##### number shop in 'Convenience store','CP','Family Mart','Lawson 108','Freshmart','108 Shop' category in 1km
dfIn['retail_1km']=dfIn.apply(lambda x: GetExtRetail_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
dfIn['residential_5km']=dfIn.apply(lambda x: GetExtResidential_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
#### number restaurant in จานด่วน เดลิเวอรี่ in 1km
dfIn['restaurant_1km']=dfIn.apply(lambda x: GetExtRestaurant_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
### number university college in 1km
dfIn['univ_1km']=dfIn.apply(lambda x: GetExtEducation_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
### number of hotel in 1 km
dfIn['hotel_1km']=dfIn.apply(lambda x: GetExtHotel_rev2(x['p_name_t'],x['lat'],x['lng']),axis=1)
print(' ======> ', dfIn)






del dfIn 


###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')