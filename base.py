import pandas as pd

class Base:
    '''Allows all of my classes to be able to extract data independently '''
    def __init__(self, **kwargs): # this __init__ will get overridden in the child classes anyway
        self.kwargs = kwargs
        self.master_stocks = self.receive_data(**kwargs)

    def receive_data(self, **kwargs): # so I don't need to write this in the child classes
        if kwargs.get("dataframe") is None and kwargs.get("path") is None:
            raise ValueError("You must pass in one of dataframe or path")
        if kwargs.get("dataframe") is not None and kwargs.get("path") is not None:
            raise ValueError("You must pass in either the dataframe or the path to a CSV but not both")

        if kwargs.get("dataframe") is not None:  # if it's a dataframe that's been passed in - use that
            return kwargs.get("dataframe")
        else:
            return pd.read_csv(kwargs.get("path"))
