from typing import no_type_check_decorator
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from Credential import *
from math import radians, cos, sin, asin, sqrt
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
import numpy as np
import psycopg2
import hdbscan
import pyodbc

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Food\\FB_Population\\'
file_name='test_data.xlsx'



def GetCoordList(x, clustered):
    dummyList=[]
    dfDummy=clustered[clustered['Cluster']==x].copy().reset_index(drop=True)
    #print(' --> ',dfDummy)
    dlist=list(dfDummy['Latitude'])
    dlng=list(dfDummy['Longitude'])
    
    dummyList=list(tuple(zip(dlist,dlng)))
    
    del dfDummy, dlist, dlng
    return dummyList

def get_centermost_point(cluster):
        #print(' cluster : ',cluster)
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y )
        #print(' ct : ',centroid)    
        centermost_point = min(cluster,key=lambda point:great_circle(point,centroid).m)
        #print(' cm : ',centermost_point)
        return tuple(centermost_point)

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

def Read_FB_Population_Dictinct_Prv():
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql="""SELECT distinct p_name_t FROM public.\"fb_population_general\" """


        dfout = pd.read_sql_query(sql, connection)

        print(' ==> ',dfout)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Write_FB_Population_Clustered(df_input):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST02;'
                            'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[FB_Population_Clustered]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[FB_Population_Clustered]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[FB_Population_Clustered] (	

      [Latitude]
      ,[Longitude]
      ,[total_population]
      ,[cluster_number]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?
  
    )""", 
      row['Latitude']
      ,row['Longitude']
      ,row['total_population']
      ,row['cluster_number']
      ,row['p_name_t']
      ,row['DBCreatedAt']
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')

#### Run this script to get distinct province names
#distinctPrv=Read_FB_Population_Dictinct_Prv()
#distinctPrv.to_csv(file_path+'prv_distinct.csv')
#print(' ===> ',distinctPrv)

prvList=['ฉะเชิงเทรา','ระยอง','ชลบุรี','กรุงเทพมหานคร','ปทุมธานี']

for prv_name in prvList:  #[:2]:
    print(' ===> ',prv_name)
    dfIn=Read_FB_Population_General_Prv(prv_name, '', '')
    dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
    print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
    #dfIn.to_csv(file_path+'pathumthani.csv')

    #X = dfIn[['lng','lat']].values
    X = dfIn[['Longitude','Latitude']].values
    print(len(X),' --- co ---- ',type(X))


    kms_per_radian = 6371.0088
    epsilon = 0.030 / kms_per_radian  # 0.1 - 649 clusters   #0.5 - 2 clusters      # (0.02)  20metres 49466 clusters

    ### DBSCAN
    db = DBSCAN ( eps = epsilon, min_samples =3 ,algorithm = 'ball_tree' , metric = 'haversine').fit ( np.radians(X) )
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    print('Number of clusters : {} '. format (num_clusters) )
    clustered = pd.concat([dfIn.reset_index(), 
                        pd.DataFrame({'Cluster':cluster_labels})], 
                        axis=1)
    clustered.drop('index', axis=1, inplace=True)
    print(' db cluster : ',clustered)

    ####  HDBSCAN
    # predictions=hdbscan.HDBSCAN(min_cluster_size=2, min_samples=1, metric='haversine',cluster_selection_epsilon=epsilon, cluster_selection_method = 'eom').fit_predict(np.radians(X))
    # clustered = pd.concat([dfIn.reset_index(), 
    #                        pd.DataFrame({'Cluster':predictions})], 
    #                       axis=1)
    # clustered.drop('index', axis=1, inplace=True)

    # clusterList=list(set(list(clustered['Cluster'].unique())))
    # num_clusters = len(clusterList)
    # print('Number of clusters : {} '. format (num_clusters) )

    #clustered.to_csv(file_path+'cluster.csv')

    clusterPd=pd.DataFrame({'cluster_number':list(clustered['Cluster'].unique())})
    clusterPd['dlist']=clusterPd.apply(lambda x: GetCoordList(x['cluster_number'],clustered),axis=1)

    clusterList=list(clusterPd['dlist'])
    clusterNumber=list(clusterPd['cluster_number'])
    #print(clusterList, ' ---  ',clusterNumber)
    del clusterPd

    clusters = pd.Series((i for i in clusterList))
    #print(clusters)

    centermost_points = clusters.map(get_centermost_point)

    lats,lons = zip(* centermost_points)
    rep_points = pd.DataFrame({ 'Longitude':lons ,'Latitude':lats, 'cluster_number':clusterNumber })
    rep_points['key']=rep_points['cluster_number']
    clustered['key']=clustered['Cluster']

    mainDf=pd.merge(clustered,rep_points, how='left',on=['key'])
    #mainDf.to_csv(file_path+'cluster.csv')

    clusterList=list(clustered['Cluster'].unique())
    subdf=pd.DataFrame(columns=['key','sum_pop'])
    for n in clusterList:  #[:3]:
        dfDummy=clustered[clustered['Cluster']==n].copy().reset_index(drop=True)    
        a=dfDummy.groupby(['Cluster'])['population'].sum().reset_index()
        #print(n,' ---  ',dfDummy,' :: ',a['population'].values[0],' ==== ',type(a['population'].values[0]))
        subdf=subdf.append({'key':n,'sum_pop':a['population'].values[0]}, ignore_index=True)
    
    clusterDf=pd.merge(subdf,rep_points, how='left',on=['key'])
    clusterDf.drop(columns=['key'], inplace=True)
    clusterDf.rename(columns={'sum_pop': 'total_population'}, inplace=True)
    print(' cdf review before : ',clusterDf.columns, ' -----  ',clusterDf.head(10))

    ## Keep only Noise cluster (-1) from mainDf 
    ## Remove sum Noise cluster from clusterDf
    ## Append Noise cluster and center assigned to be location of each Noise point to mainDf

    #####  Separate cluster number -1 and replace its original location
    mainNoiseDf=mainDf[mainDf['cluster_number']==-1].copy().reset_index(drop=True)
    print('mainNoiseDf (column) : ',mainNoiseDf.columns,' :: ',mainNoiseDf.head(10))
    mainNoiseDf=mainNoiseDf[['population','Longitude_x','Latitude_x','cluster_number']]
    mainNoiseDf.rename(columns={'population':'total_population','Longitude_x':'Longitude','Latitude_x':'Latitude'},inplace=True)
    #print('mainNoiseDf : ',mainNoiseDf)
    
    clusterDf=clusterDf[clusterDf['cluster_number']!=-1].copy().reset_index(drop=True)
    #print(' cdf : ',clusterDf)
    clusterDf=clusterDf.append(mainNoiseDf).reset_index(drop=True)

    #####  Separate cluster number 0 and replace its original location
    mainNoiseDf=mainDf[mainDf['cluster_number']==0].copy().reset_index(drop=True)
    mainNoiseDf=mainNoiseDf[['population','Longitude_x','Latitude_x','cluster_number']]
    mainNoiseDf.rename(columns={'population':'total_population','Longitude_x':'lon','Latitude_x':'lat'},inplace=True)
    #print('mainNoiseDf : ',mainNoiseDf)

    clusterDf=clusterDf[clusterDf['cluster_number']!=0].copy().reset_index(drop=True)
    #print(' cdf : ',clusterDf)
    clusterDf=clusterDf.append(mainNoiseDf).reset_index(drop=True)
    #print(' cdf 2 : ',clusterDf)




    clusterDf['p_name_t']=prv_name
    clusterDf['DBCreatedAt']=nowStr
    #print(' cdf 2 : ',clusterDf)
    del mainNoiseDf


    print(' cluster ', clusterDf)
    clusterDf.to_csv(file_path+prv_name+'_cluster_30m_min3_db.csv')
    Write_FB_Population_Clustered(clusterDf)



del dfIn, clusterList, mainDf, subdf, dfDummy, a, clusterDf, clusters, clustered, X
del rep_points, centermost_points

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')