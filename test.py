import pandas as pd
from sqlalchemy import create_engine, table


username=username
password=password
ipaddress='127.0.0.1'
dbname='database_name'
port='5432'
url = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=username,password=password,ipaddress=ipaddress,port=port,dbname=dbname))
engine =create_engine(url, pool_size=50, echo=False)
is_table_exist=engine.has_table('back_up')
if is_table_exist:
    df1=pd.read_sql_table('back_up',engine)
    df= pd.read_csv('csv file path.csv',encoding= 'unicode_escape',error_bad_lines=False )
    df.columns =map(str.upper, df.columns)
    df.columns=df.columns.str.replace('[#,@,&,%,_,' ']','')
    df.columns = df.columns.str.strip()
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df.index+=1
    index_names = df[ df['ERRORS'] == 'ERRORS' ].index
    df.drop(index_names, inplace = True)
    df.index=df.index.rename('SR-NO')
    df['LOCATION']=df['HOST-NAME']
    df.loc[df['LOCATION'].str.startswith('IN'), 'LOCATION'] = 'INDIA'
    df.loc[df['LOCATION'].str.startswith('IL'), 'LOCATION'] = 'ISRAEL'
    df1=pd.read_sql_table('back_up',engine)
    frames=[df1,df]
    result=pd.concat(frames)
    del result['SR-NO']
    result.to_sql('back_up',con = engine,if_exists = 'replace')
    df2=pd.read_sql_table('back_up',engine)
    del df2['index']
    df2.index+=1
    df2.index=df2.index.rename('SR-NO')
    df2.to_sql('table name',con = engine,if_exists = 'replace')

else:
    df= pd.read_csv('csv file path.csv',encoding= 'unicode_escape',error_bad_lines=False )
    df.columns =map(str.upper, df.columns)
    df.columns=df.columns.str.replace('[#,@,&,%,_,' ']','')
    df.columns = df.columns.str.strip()
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df.index+=1
    index_names = df[ df['ERRORS'] == 'ERRORS' ].index
    df.index=df.index.rename('SR-NO')
    df.drop(index_names, inplace = True)
    df['LOCATION']=df['HOST-NAME']
    df.loc[df['LOCATION'].str.startswith('IN'), 'LOCATION'] = 'INDIA'
    df.loc[df['LOCATION'].str.startswith('IL'), 'LOCATION'] = 'ISRAEL'
    df.to_sql('table name',con = engine,if_exists = 'replace')

