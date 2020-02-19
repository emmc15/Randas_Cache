# Randas Cache
Cache Pandas Dataframe with Redis

Randas cache creates the abiility to decorate functions or methods that return a pandas Dataframe and store that in a redis database

```python
from Randas_Cache import Randas_Cache
import redis
r = redis.Redis(host='localhost', port=6379, db=0)


cache = Randas_Cache(r)

@cache.cache_df
def test(key, num):
    df=pd.DataFrame({'A':[1,2,3]})
    return df


test('key', 4)
```

When we run the function above, the first time it is ran, it will run the process. thereafter, if the function name and params are the same, the dataframe will be returned from redis rather than be generated again

The conversion of being to store pandas dataframe in redis is handled by pyarrow (https://arrow.apache.org/docs/python/memory.html#pyarrow-buffer)
