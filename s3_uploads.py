import boto3
import os

if __name__ =='__main__':

    s3 = boto3.resource("s3")

    bucket = s3.Bucket("kestrl-data-intern") # I don't remember inputting credentials.. it's already stored in the .aws file on my local machine?
    # TODO loop through all the files in data-sources and upload them
    for data_source in os.listdir('data-sources'):
        bucket.upload_file(Filename=f"data-sources/{data_source}", Key=f"data-sources/{data_source}")
    # TODO delete all the files on the local machine once they've been uploaded?
