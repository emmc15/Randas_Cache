import pandas as pd
import pyarrow as pa
import redis
import functools


class Randas_Cache:

    def __init__(self, redis_instance):
        # Checks that a redis object has been passed
        # https://stackoverflow.com/questions/57949871/how-to-set-get-pandas-dataframes-into-redis-using-pyarrow/57986261#57986261
        if isinstance(redis_instance, redis.client.Redis):

            # Sets the redis object as an attribute of this class
            self.cache_container = redis_instance

            # pyarrow serializer, uses the default serialization method and assigns as an attribute
            self.context = pa.default_serialization_context()

            # leading for reference in db
            self.leading_key = 'Randas_Cache'
        else:
            raise AttributeError(
                "Redis instance's decode_responses must be set True. Use StrictRedis(..., decode_responses=True)")

    def key_generator(self, func, *args, **kwargs):
        """
        Generates a key for redis to cache based on the function it will decorate as well as the inputs to that funciton

        Returns:
            str()
        """
        params = [self.leading_key, func.__name__]
        if len(args) > 0:
            args_given = [str(i) for i in args]
            args_given = ':'.join(args_given)
            params.append(args_given)
        if len(kwargs) > 0:
            params.append(str(kwargs))
        return ":".join(params)

    def cache_df(self, func, timeout=50):
        """
        Decorater function for caching functions that return a dataframe'

        Parameters:
            func = python function, is the input due to wrapping
            timeout = redis instance attribute for when time out to occue with function
        Returns:
            pd.DataFrame()
        """
        @functools.wraps(func)
        def wrapper_df_decorator(*args, **kwargs):
            # generate key based on called function
            key = self.key_generator(func, *args, *kwargs)

            # check if exists in redis
            if self.cache_container.exists(key) == 1:
                # Pulls data from redis, deserialses and returns the dataframe
                value = self._deserialize(key)
            else:
                # Runs the function that was decorated
                value = func(*args, **kwargs)
                # if function is dataframe, insert into database
                if isinstance(value, pd.DataFrame):
                    self._serialize(key, value)

            return value
        return wrapper_df_decorator

    def _deserialize(self, key):
        pull_value = self.cache_container.get(key)
        return self.context.deserialize(pull_value)

    def _serialize(self, key, df):
        hashed_df = self.context.serialize(df).to_buffer().to_pybytes()
        self.cache_container.set(key, hashed_df)


if __name__ == "__main__":
    pass
