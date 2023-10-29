from datetime import datetime as dt
from time import time
import pandas as pd

from mods.survey_mysql import MysqlSurvey
from mods.survey_mongodb import MongoSurvey
from mods.survey_spark import SparkSurvey

print(
    f'''
    \n
    #######################################

       データサーバのパフォーマンスを測定  
       開始時刻：{dt.now()}
    \n
    '''
)

# sample_df = pd.read_csv('./data/sample_dummy_data.csv')

# print('-------1. MySQLパフォーマンス調査-------\n')
# mysql_start = time()
# print('# connect')
# mysql_survey = MysqlSurvey()
# # print('# table')
# # mysql_survey.create_mysql_table()
# print('# insert')
# mysql_survey.insert_mysql_db(sample_df)
# print('# search')
# mysql_survey.search_mysql_db()
# print('# close')
# mysql_survey.close()

# mysql_end = time()
# mysql_diff = mysql_start - mysql_end
# print('MySQL挿入・検索合計時間',mysql_diff,'秒\n')

# print('-------2. MongoDBパフォーマンス調査-------\n')
# mongo_start = time()
# print('# connect')
# mongo_survey = MongoSurvey()
# print('# insert')
# mongo_survey.insert_mongo_db(sample_df)
# print('# search')
# mongo_survey.search_mongo_db()
# mongo_end = time()
# mongo_diff = mongo_start - mongo_end
# print('MongoDB挿入・検索合計時間',mongo_diff,'秒\n')

print('-------3. Sparkパフォーマンス調査（parquet）-------\n')
spark_start = time()
print('# connect')
spark_survey = SparkSurvey()
dummy_data = spark_survey.get_csv_data()
print('# insert')
spark_survey.create_parquet_file(dummy_data)
print('# search')
spark_survey.search_parquet_data()
spark_end = time()
spark_diff = spark_start - spark_end
print('Spark挿入・検索合計時間',spark_diff,'秒\n')

# print('-------3. Sparkパフォーマンス調査（Hive-table）-------\n')
# spark_start = time()
# print('# connect')
# spark_survey = SparkSurvey()
# dummy_data = spark_survey.get_csv_data()
# print('# insert')
# spark_survey.create_hive_table(dummy_data)
# print('# search')
# spark_survey.search_hive_table_data()
# spark_end = time()
# spark_diff = spark_start - spark_end
# print('Spark挿入・検索合計時間',spark_diff,'秒\n')

# print('-------3. Sparkパフォーマンス調査（hdfs-parquet）-------\n')
# spark_start = time()
# print('# connect')
# spark_survey = SparkSurvey()
# dummy_data = spark_survey.get_csv_data()
# print('# insert')
# spark_survey.create_hdfs_parquet_file(dummy_data)
# print('# search')
# spark_survey.search_hdfs_parquet_data()
# spark_end = time()
# spark_diff = spark_start - spark_end
# print('Spark挿入・検索合計時間',spark_diff,'秒\n')

# print('-------3. Sparkパフォーマンス調査（hdfs-hive-table）-------\n')
# spark_start = time()
# print('# connect')
# spark_survey = SparkSurvey()
# dummy_data = spark_survey.get_csv_data()
# print('# insert')
# spark_survey.create_hive_table(dummy_data)
# print('# search')
# spark_survey.search_hive_table_data()
# spark_end = time()
# spark_diff = spark_start - spark_end
# print('Spark挿入・検索合計時間',spark_diff,'秒\n')

print(
    f'''
    \n
    #######################################

       データサーバのパフォーマンスを測定  
       終了時刻：{dt.now()}
    \n
    '''
)
