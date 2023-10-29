import pandas as pd
import numpy as np

def get_displacement(num,step):
    y_list = []
    for i in range(int(num/step)):
        x = np.linspace(-10,10,step)
        y = x*x/10
        y_list += y.tolist()
    return y_list

def get_load(num,step,freq):
    y_list = []
    x = np.linspace(0,int(step/100),int(num))
    y = np.sin(2**np.pi*x*freq)
    y_list = y.tolist()
    return y_list

SUMPLE_NUM = 10000000
dummy_data = pd.DataFrame(columns=['timestamp','sequential_number','shot_number','sequential_number_by_shot','displacement','load01','load02'])

# iso8601形式のJSTタイムスタンプ文字列を付与
dummy_data['timestamp'] = pd.date_range(start='2022-12-9', periods=SUMPLE_NUM, freq='10U')
dummy_data['timestamp'] = dummy_data['timestamp'].apply(lambda s: s.isoformat())
# dummy_dataから0始まりのsequential_numberをいれる
sequential_number = pd.RangeIndex(start=0, stop=len(dummy_data.index), step=1)
dummy_data['sequential_number'] = sequential_number
# dummy_dataから1始まりで1000ごとにインクリメントする。
dummy_data['shot_number'] = pd.RangeIndex(start=1, stop=len(dummy_data.index)+1, step=1)
dummy_data['shot_number'] = dummy_data['shot_number'].apply(lambda s: int(s/1000) if s%1000 == 0 else s//1000+1)
# dummy_dataから0から1000までをインクリメントする。
dummy_data['sequential_number_by_shot'] = pd.RangeIndex(start=0, stop=len(dummy_data.index), step=1)
dummy_data['sequential_number_by_shot'] = dummy_data['sequential_number_by_shot'].apply(lambda s: int(s%1000))
# displacementをいれる
dummy_data['displacement'] = get_displacement(num=SUMPLE_NUM,step=1000)
# load01、load02ダミーの荷重をいれる
# freqは、10000の1000ごとに周期をだすため10
dummy_data['load01'] = get_load(num=SUMPLE_NUM,step=1000,freq=10)
dummy_data['load02'] = get_load(num=SUMPLE_NUM,step=1000,freq=20)

# fig = plt.plot()
# plt.plot(dummy_data['sequential_number'],dummy_data['load01'])
# plt.plot(dummy_data['sequential_number'],dummy_data['displacement'])
# plt.show()

dummy_data.to_csv('../data/sample_dummy_data.csv')