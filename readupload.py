import pandas as pd
import datetime
import boto3
from pathlib import Path

class ReadUpload:
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

    def upload_s3(self, **kwargs): # how do I make it so you can make this choice to upload at runtime/in the console?
        # TODO how to avoid overwriting my data with test data (one row), I want to include a prefix that can be set at run-time
        post_internet_data_file_name = kwargs.get("s3_filename",
                                                  None) # this never gets called because I always use s3_filename as a parameter
        if kwargs.get("prefix"):
            post_internet_data_file_name = kwargs['prefix'] + post_internet_data_file_name



        if post_internet_data_file_name:
            s3 = boto3.resource("s3")
            bucket = s3.Bucket("kestrl-data-intern")

            Path(post_internet_data_file_name).parent.mkdir(exist_ok=True, parents=True)
            self.master_stocks.to_csv(post_internet_data_file_name) # is the self a ReadUpload object or is it passed in?
            bucket.upload_file(Filename=f"{post_internet_data_file_name}",
                               Key=f"intermediate-data/{str(datetime.datetime.now()).replace(':', ' ')}-{post_internet_data_file_name}")
        else:
            print(f'no post internet path was passed in: {post_internet_data_file_name}') # if s3_filename isn't passed in
        # TODO delete the csv file from local file?
