import json
import csv
import boto3
import botocore
import io
from io import BytesIO as StringIO
import time


REGION = "us-east-2"

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    """Entry Point for Lambda"""

    comprehend = boto3.client(service_name='comprehend')

    csvio = StringIO()
    writer = csv.writer(csvio)

    # Process Queue
    for record in event['Records']:
        body = json.loads(record['body'])
        id_ = body['id']
        review = body['review']
        payload = comprehend.detect_sentiment(Text=review, LanguageCode='en')
        sentiment = payload['Sentiment']
        writer.writerow([str(id_),str(review),str(sentiment)])

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "sentiment_analysis/"+str(timestr)+"_sentiment.csv"

    s3.Object("dynamo-lambda-19042020", filename).put(Body=csvio.getvalue())
    print("File : %s inserted to S3"%(str(timestr)+"_sentiment.csv"))
    
