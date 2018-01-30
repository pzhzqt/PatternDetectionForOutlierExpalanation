import pandas as pd
from sklearn.linear_model import LinearRegression

def fitlinear(d,f,v,agg):
    for row in pd.DataFrame.drop_duplicates(d[f]).itertuples():
        