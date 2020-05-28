import pandas as pd
import pyarrow as pa
import numpy as np
import redis
import functools
import pickle
import json
import hashlib

class RandasCache(object):

    def __init__(self, redis_instance, key=None, hash_keys=False):
        # Checks that a redis object has been passed
        # https://stackoverflow.com/questions/57949871/how-to-set-get-pandas-dataframes-into-redis-using-pyarrow/57986261#57986261
        if isinstance(redis_instance, redis.client.Redis) !=True:
            raise AttributeError(
                "Did not recieve an Redis Object, instead recieved {}".format(type(redis_instance)))


        # Sets the redis object as an attribute of this class
        self.cache_container = redis_instance

        # pyarrow serializer, uses the default serialization method
        self.context = pa.default_serialization_context()

        # leading for reference in db, can be used to identify the caching object in redis
        self.leading_key = 'RandasCache'

        # Key to identify object/state of object,
        # if None, key will try to be generated from inputs of function
        self.key=key

        assert isinstance(hash_keys, bool), "[ERROR]: hash_keys should be a bool"
        self.hash_keys=hash_keys

        # storing of keys in the object so as to know how to retrieve the value
        self.keys={}

    #------------------------------------------------------------------------------------
    # Internal Methods for mananging posting and getting from redis
    #------------------------------------------------------------------------------------
    @staticmethod
    def _hashing_key(*args):
        """
        Creates a hash of the object given to create unique id
        using the sha256 algorith
        Parameters:
        -----------
            args=must be able represented as a str
        Returns:
        --------
            str(), hash of the inputs
        """
        # flattens args in case of a list is in the value
        values=[]
        for i in args:
            if isinstance(i, list):
                for j in i:
                    list_values=[]
                    # Checks if value in list is none
                    if j is not None:
                        # Adds to cleaned list values
                        list_values.append(str(j))
                # Sort inner list before adding to params values
                if len(list_values)>0:
                    sorted(list_values)
                    for x in list_values:
                        values.append(x)
            else:
                values.append(str(i))

        #Creates a key for redis cache
        # URL: https://www.pythoncentral.io/hashing-strings-with-python/
        key=', '.join(values)
        generated_key=hashlib.sha256(key.encode())
        generated_key=generated_key.hexdigest()
        return generated_key


    def key_generator(self, func, *args, **kwargs):
        """
        Generates a key for redis to cache based on the function
        it will decorate as well as the inputs to that function
        Returns:
            str()
        """
        leading = f"{self.leading_key}-{func.__name__}"

        params=[]
        # Cleaning up args
        if len(args) > 0:
            args_given = [str(i) for i in args]
            args_given = ':'.join(args_given)
            params.append(args_given)

        # Cleaning up kwargs
        if len(kwargs) > 0 and self.hash_keys==False:
            for i in kwargs.keys():
                params.append(f"({i}-{kwargs[i]})")

        # Hashes key with sha256 to generate a unique id
        if self.hash_keys==True:
            # Takes list of params and joins into unique id string
            params=RandasCache._hashing_key(params)
            key=[leading, params]

        else:
            # Inserts the leading key and func string as prefix
            key=params.insert(0, leading)

        return ":".join(key)

    def _deserialize(self, key):
        """
        Method for deserializing python object
        Parameters:
        -----------
            key=value to use in the redis cache
        Returns:
        --------
            python object
        """
        pull_value = self.cache_container.get(key)

        # If key found, and not passed through object, return serialized object
        if key not in self.keys.keys():
            return pickle.loads(pull_value)


        if self.keys[key]=='pickle':
            value=pickle.loads(pull_value)
        elif self.keys[key]=='pyarrow':
            value=self.context.deserialize(pull_value)
        elif self.keys[key]=='json':
            value=json.loads(pull_value)
        return value


    def _serialize(self, key, value, method='pickle'):
        """
        Method for serializing python object
        Parameters:
        -----------
            key=value to use in the redis cache
            method=str(pickle or pyarrow), method to deserialize the object
        Returns:
        --------
            python object
        """

        if method=='pickle':
            hashed_value=pickle.dumps(value)
            self.keys[key]='pickle'

        elif method=='pyarrow':
            hashed_value = self.context.serialize(value).to_buffer().to_pybytes()
            self.keys[key]='pyarrow'

        elif method=='json':
            hashed_value = json.dumps(value)
            self.keys[key]='json'

        self.cache_container.set(key, hashed_value)

    #------------------------------------------------------------------------------------
    # GET and POST methods to send and retrive objects from cache
    #------------------------------------------------------------------------------------
    def get(self, key):
        """
        Returns object from the redis database if key exists
        """
        if self.cache_container.exists(key) == 1:
            return self._deserialize(key)
        else:
            raise ValueError("No key {} found in object".format(key))


    def post(self, key, values, serialization):
        """
        Posts key,value to redis where its serilaized by the serialization param
        Parameters:
        -----------
            key=str()
            values=python object
            serialization=str(), of which is 'pickle', 'json', 'pyarrow'
        Returns:
        --------
            python object
        """
        assert isinstance(key, str)
        assert isinstance(serialization,str)
        assert serialization in ['pickle', 'json', 'pyarrow']

        self._serialize(key, values, serialization)
    #------------------------------------------------------------------------------------
    # Caching Decorators to be used over functions
    #------------------------------------------------------------------------------------

    def cache(self, func):
        """
        General Decorater function for caching functions, will attempt to cache with the
        correct serialization based on type, else use pickle
        Parameters:
            func = python function, is the input due to wrapping
        Returns:
            Python Object
        """
        @functools.wraps(func)
        def wrapper_df_decorator(*args, **kwargs):
            # generate key based on called function
            if self.key==None:
                key=self.key_generator(func, *args, **kwargs)
            else:
                key=self.key_generator(func, self.key)

            # check if exists in redis
            if self.cache_container.exists(key) == 1:
                # Pulls data from redis, deserialses and returns the dataframe
                value = self._deserialize(key)
            else:
                # Runs the function that was decorated
                value = func(*args, **kwargs)
                # if function is dataframe, insert into database
                if isinstance(value, pd.DataFrame) or isinstance(value, np.array):
                    self._serialize(key, value, method='pyarrow')

                else:
                    self._serialize(key, value, method='pickle')



            return value
        return wrapper_df_decorator


    def json_cache(self, func):
        """
        Decorater function for caching functions with json serialization
        Recommended to be used with dict style objects
        Refer to the python json api for more info
        Parameters:
            func = python function, is the input due to wrapping
        Returns:
            Python Object
        """
        @functools.wraps(func)
        def wrapper_df_decorator(*args, **kwargs):
            # generate key based on called function
            if self.key==None:
                key=self.key_generator(func, *args, **kwargs)
            else:
                key=self.key_generator(func, self.key)

            # check if exists in redis
            if self.cache_container.exists(key) == 1:
                # Pulls data from redis, deserialses and returns the dataframe
                value = self._deserialize(key)
            else:
                # Runs the function that was decorated
                value = func(*args, **kwargs)
                self._serialize(key, value,method='json')

            return value
        return wrapper_df_decorator

    def pyarrow_cache(self, func):
        """
        Decorater function for caching functions with pyarrow serialization
        Recommended to be used with pandas, numpy or any table style object
        Refer to pyarrow for more specification
        Parameters:
            func = python function, is the input due to wrapping
        Returns:
            Python Object
        """
        @functools.wraps(func)
        def wrapper_df_decorator(*args, **kwargs):
            # generate key based on called function
            if self.key==None:
                key=self.key_generator(func, *args, **kwargs)
            else:
                key=self.key_generator(func, self.key)

            # check if exists in redis
            if self.cache_container.exists(key) == 1:
                # Pulls data from redis, deserialses and returns the dataframe
                value = self._deserialize(key)
            else:
                # Runs the function that was decorated
                value = func(*args, **kwargs)
                self._serialize(key, value, method='pyarrow')

            return value
        return wrapper_df_decorator



if __name__ == "__main__":
    pass
