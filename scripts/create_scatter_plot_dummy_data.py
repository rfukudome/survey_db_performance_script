import pandas as pd
import numpy as np
import random as rd
import matplotlib.pyplot as plt

SUMPLE_NUM = 100000
dummy_data = pd.DataFrame(columns=['shot_number','timestamp','feature01','feature02'])

# iso8601形式のJSTタイムスタンプ文字列を付与
dummy_data['timestamp'] = pd.date_range(start='2022-12-9', periods=SUMPLE_NUM, freq='10U')
dummy_data['timestamp'] = dummy_data['timestamp'].apply(lambda s: s.isoformat())
# dummy_dataから1始まりで1000ごとにインクリメントする。
dummy_data['shot_number'] = pd.RangeIndex(start=1, stop=len(dummy_data.index)+1, step=1)
dummy_data['shot_number'] = dummy_data['shot_number'].apply(lambda s: int(s/1000) if s%1000 == 0 else s//1000+1)
# ランダムな浮動小数点をfeature01にいれる
dummy_data['feature01'] = dummy_data['timestamp'].apply(lambda s: round(rd.uniform(-3,3),2))
# feature01の浮動小数点を2倍にしていれる
dummy_data['feature02'] = dummy_data['feature01'].apply(lambda s: s*2)

# データ1ショットごと（計100）に抽出
shot_num = [x for x in range(0,SUMPLE_NUM,1000)]
scatter_plot_dummy_data = dummy_data.copy()
scatter_plot_dummy_data = scatter_plot_dummy_data.iloc[shot_num]

scatter_plot_dummy_data.to_csv('./sample_scatter_plot_dummy_data.csv')