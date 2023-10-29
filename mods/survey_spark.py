from datetime import datetime as dt
from time import time
from os import path
from pyspark.sql import SparkSession



class SparkSurvey:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        # self.spark = SparkSession \
        #     .builder \
        #     .appName("Python Spark SQL basic example") \
        #     .config("spark.some.config.option", "some-value") \
        #     .getOrCreate()
        warehouse_path = path.abspath('spark_data')
        # warehose_url https://sparkbyexamples.com/apache-hive/pyspark-save-dataframe-to-hive-table/
        self.spark_warehouse = SparkSession \
            .builder \
            .appName("SparkByExamples.com") \
            .config("spark.sql.warehouse.dir", warehouse_path) \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()
        
    def get_csv_data(self):
        csv_path = path.join('data','sample_dummy_data.csv')
        dummy_df = self.spark.read.load(csv_path,format="csv",inferSchema="true",header="true")
        # dummy_df.show()
        return dummy_df

    def create_parquet_file(self,dummy_df):
        spark_insert_start = time()
        parquet_path = path.join('data','sample_dummy_data.parquet')
        dummy_df.write.parquet(parquet_path)
        spark_insert_end = time()
        spark_diff = spark_insert_start - spark_insert_end 
        print('Sparkのinsert経過時間\n',spark_diff,'秒\n')

    def search_parquet_data(self):
        spark_search_start = time()
        parquet_path = path.join('data','sample_dummy_data.parquet')
        rows = self.spark.read.parquet(parquet_path)
        # 一時的にHiveテーブルを作成する
        # parquetFile.createOrReplaceTempView("TEST_TABLE")
        # rows = self.spark.sql("SELECT * FROM TEST_TABLE")
        spark_search_end = time()
        spark_diff = spark_search_start - spark_search_end
        print('Sparkのsearch経過時間\n',spark_diff,'秒\n') 
        print('Sparkの検索結果一覧\n')
        rows.show()

    def create_hive_table(self,dummy_df):
        spark_insert_start = time()
        dummy_df.write.mode('overwrite').saveAsTable('TEST_TABLE')
        # dummy_df.write.mode('overwrite').option("path","hdfs://localhost:9000/data/hive/").saveAsTable('TEST_TABLE')
        spark_insert_end = time()
        spark_diff = spark_insert_start - spark_insert_end 
        print('Sparkのinsert経過時間\n',spark_diff,'秒\n')

    def search_hive_table_data(self):
        spark_search_start = time()
        rows = self.spark_warehouse.read.table("TEST_TABLE")
        # rows = self.spark_warehouse.read.option("path","hdfs://localhost:9000/data/hive/").table("TEST_TABLE")
        spark_search_end = time()
        spark_diff = spark_search_start - spark_search_end
        print('Sparkのsearch経過時間\n',spark_diff,'秒\n') 
        print('Sparkの検索結果一覧\n')
        rows.show()

    def create_hdfs_parquet_file(self,dummy_df):
        spark_insert_start = time()
        dummy_df.write.parquet('hdfs://localhost:9000/data/sample_dummy_data.parquet')
        spark_insert_end = time()
        spark_diff = spark_insert_start - spark_insert_end 
        print('Sparkのinsert経過時間\n',spark_diff,'秒\n')

    def search_hdfs_parquet_data(self):
        spark_search_start = time()
        rows = self.spark.read.parquet('hdfs://localhost:9000/data/sample_dummy_data.parquet')
        spark_search_end = time()
        spark_diff = spark_search_start - spark_search_end
        print('Sparkのsearch経過時間\n',spark_diff,'秒\n') 
        print('Sparkの検索結果一覧\n')
        rows.show()

# hadoop_url https://atmarkit.itmedia.co.jp/ait/articles/1701/01/news015.html

    