import logging
logging.basicConfig(filename = 'file.log',
        level = logging.DEBUG,
        format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')

def csv_to_table():
        '''
        connection to database using sqlalchemy pakage 
        read csv using pandas 
        made some changes as per requirement in dataframe
        import datframe into database using pandas, pass connection parameter while importing csv sheet into database table
        '''
        username=username
        password=password
        ipaddress='127.0.0.1'
        dbname='database_name'
        port='5432'
        file_name='csv_file_name'
        try:
            import pandas as pd
            from sqlalchemy import create_engine, table
            try:
                postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=username,password=password,ipaddress=ipaddress,port=port,dbname=dbname))
                cnx=create_engine(postgres_str)
                cnx.connect()
                try:
                    df= pd.read_csv(file_name,encoding='unicode_escape')
                    try:
                        df.columns =map(str.upper, df.columns)
                        df.columns=df.columns.str.replace('[#,@,&,%,_,' ']','')
                        df.columns = df.columns.str.strip()
                        df=df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                        df.index+=1
                        df.to_sql('merge',con = cnx,if_exists = 'replace')
                    except:
                        pass
                except OSError as e:
                    logging.warning('file {0} not exist'.format(file_name))

            except:
                logging.warning('error while connecting with postgres database with following credintial= databasename:{0}, username:{1}, ipaddress:{2}, port:{3}'.format(dbname,username,ipaddress,port))
        except:
          logging.warning('required module not installed')
       

csv_to_table()

