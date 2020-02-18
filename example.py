import pandas as pd
import pyarrow as pa
import redis
from Ranadas_Cache import Randas_Cache

r = redis.Redis(host='localhost', port=6379, db=0)


cache = Randas_Cache(r)

@cache.cache_df
def test(key, num):
    df=pd.DataFrame({'A':[1,2,3]})
    return df


test('key', 4)