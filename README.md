# データサーバ性能調査スクリプト
## 調査概要
データ分析基盤にてIoTGWのデータ保存に利用するデータサーバの選定をするための調査になる。  
検証に利用するデータは、Pythonスクリプトにて作成したCSVデータを使用する。  
検証方法は下記になる。
 - データ1000万件を挿入。
 - データ1000万件を検索。

## スクリプト概要
 - db_performance_survey.py

調査対象製品は、下記になる。
 - mysql
 - mongodb
 - spark and hadoop 

調査対象の仮想基盤環境は、下記になる。
 - ESXi 7.0u3g
 - CPU:i7-9700 3.60Hz(core×4)
 - Memory: 16GB

調査対象のVMは、下記になる  
  ・MySQL
  - version: 
  - OS
    - Description:    Ubuntu 22.04.1 LTS
    - Codename:       jammy
    - CPU:2 vCPUs
    - Memory: 4GB
    - Storage: 400GB

 ・MongoDB
  - version: db version v6.0.3
  - OS
    - Description:    Ubuntu 22.04.1 LTS
    - Codename:       jammy
    - CPU:2 vCPUs
    - Memory: 4GB
    - Storage: 400GB

 ・Spark and Hadoop
  - spark_version:  version 3.3.1
  - hadoop_version: Hadoop 3.3.4
  - OS
    - Description:    Ubuntu 22.04.1 LTS
    - Codename:       jammy
    - CPU:2 vCPUs
    - Memory: 4GB
    - Storage: 400GB
