from datetime import datetime as dt
from time import time
import resource
import gc
from sqlalchemy import create_engine
import mysql.connector as mysqldb 
from mysql.connector import Error

class MysqlSurvey:
    def __init__(self):
        self.DB_USER = 'db-user'
        self.DB_PASS = 'Passw0rd'
        self.DB_NAME = 'TEST_DB'
        self.TABLE_NAME = 'TEST_TABLE'
        self.MYSQL_IP = 'XXX.XXX.XXX'
        url = f'''mysql://{self.DB_USER}:{self.DB_PASS}@{self.MYSQL_IP}:3306/{self.DB_NAME}?charset=utf8'''
        self.engine = create_engine(url)


        try:
            self.connection = mysqldb.connect(
                host=self.MYSQL_IP,
                port='3306',
                user=self.DB_USER,
                passwd=self.DB_PASS,
                database=self.DB_NAME
            )
            self.connection.ping(reconnect=True)
            self.cursor = self.connection.cursor()
        except Error as err:
            print(f"Error: '{err}'") 

        #メモリ制約
        resource.setrlimit(resource.RLIMIT_DATA, (7 * 1024 ** 3, -1))
        # 書き換わっていることを確認
        print('memory limit',resource.getrlimit(resource.RLIMIT_DATA))
        
        
    # def create_mysql_table(self):
    #     query= \
    #     f'''
    #         CREATE TABLE {self.DB_NAME}.{self.TABLE_NAME}(
    #             timestamp timestamp,
    #             sequential_number int,
    #             shot_number int,
    #             sequential_number_by_shot int,
    #             displacement numeric,
    #             load01 numeric,
    #             load02 numeric
    #         );
    #     '''
    #     try:
    #         self.cursor.execute(query)
    #     except Error as err:
    #         print(f"Error: '{err}'")


    def insert_mysql_db(self,df):
        dummy_df = df.copy()
        dummy_df['timestamp'] = dummy_df['timestamp'].apply(lambda s: dt.fromisoformat(s))
        del df
        gc.collect()
        print('# dummy_data\n',dummy_df)
        # query= \
        # f'''
        #     INSERT INTO {self.TABLE_NAME}
        #     (
        #         timestamp
        #         sequential_number,
        #         shot_number,
        #         sequential_number_by_shot,
        #         displacement,
        #         load01,
        #         load02,
        #     )
        #     VALUES
        #     (
        #         %s,%s,%s,%s,%s,%s,%s
        #     )
        # '''
        mysql_insert_start = time()
        try:
            dummy_df.to_sql(self.TABLE_NAME, con=self.engine, if_exists='replace', chunksize=1000)
            # self.cursor.executemany(query,sample_data)
            # self.connection.commit()
        except Error as err:
            print(f"Error: '{err}'")
        
        mysql_insert_end = time()
        mysql_insert_diff= mysql_insert_start - mysql_insert_end
        print('MySQLのinsert経過時間\n',mysql_insert_diff,'秒\n')


    def search_mysql_db(self):
        query= \
        f'''
            SELECT * FROM {self.TABLE_NAME} 
        '''
        mysql_search_start = time()
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
        except Error as err:
            print(f"Error: '{err}'")
        
        mysql_search_end = time()
        mysql_search_diff= mysql_search_start - mysql_search_end
        print('MySQLのsearch経過時間\n',mysql_search_diff,'秒\n')
        print('MySQL取得データ件数\n',len(rows))
    
    def close(self):
        self.cursor.close()
        self.connection.close()
        