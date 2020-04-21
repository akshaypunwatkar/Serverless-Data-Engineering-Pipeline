import boto3
import csv

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    region='us-east-2'
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    obj = s3.get_object(Bucket=bucket,Key=key)

    rows = obj['Body'].read().split('\n')

    table = dynamodb.Table('amazon-review')

    csv_reader = csv.reader(rows, delimiter=',', quotechar='"')


    with table.batch_writer() as batch:
        for row in csv_reader:
            batch.put_item(Item={
                "id":row[0],
                "review":row[1].replace(',','')
            })

    print("%s records inserted into amazon-review"%(len(rows)))        
