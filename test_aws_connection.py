from dotenv import load_dotenv
import boto3
import os

load_dotenv()  # .env 파일 읽어서 환경변수로 등록

ec2 = boto3.client(
    "ec2",
    region_name=os.getenv("AWS_DEFAULT_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

response = ec2.describe_instances()
for r in response["Reservations"]:
    for i in r["Instances"]:
        print(i["InstanceId"], i["State"]["Name"], i["InstanceType"])