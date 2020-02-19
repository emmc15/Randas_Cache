# Randas Cache
Cache Pandas Dataframe with Redis

Randas cache creates the abiility to decorate functions or methods that return a pandas Dataframe and store that in a redis database

The conversion of being to store pandas dataframe in redis is handled by pyarrow (https://arrow.apache.org/docs/python/memory.html#pyarrow-buffer)
