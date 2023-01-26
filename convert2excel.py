import os
import pandas as pd

# list csv file
file = os.listdir(os.path.join(os.getcwd()))
file = [f for f in file if os.path.splitext(f)[1] in ['.csv']]

# replace \\n and \n to space
for filename in file:
    if filename not in ['summary_performance.csv']:
        print(f'{filename} start')
        df = pd.read_csv(filename)
        df['ztrus_info'] = df['ztrus_info'].replace({r'\s+$': '', r'^\s+': ''}, regex=True).replace(r'\n',  ' ', regex=True)
        df['ztrus_info'] = df['ztrus_info'].replace({r'\s+$': '', r'^\s+': ''}, regex=True).replace(r'\\n',  ' ', regex=True)
        filenameindexzero = os.path.splitext(filename)[0]
        df.to_excel(f'{filenameindexzero}.xlsx', index=False)
        print(f'{filename} finish')