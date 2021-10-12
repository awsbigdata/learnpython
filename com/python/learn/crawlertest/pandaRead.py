import json
import pandas as pd

df = pd.DataFrame({'length':[1,2,3,'test'], 'width':[10, 20, 30,'hello']})

for row_dict in df.to_dict(orient="records"):
    print(row_dict)