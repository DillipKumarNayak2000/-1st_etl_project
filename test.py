import base64
import json

from boto3 import Session
from botocore.exceptions import ClientError


def get_secret(secret_name, sm_client):
    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
        if e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        print(get_secret_value_response)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


region = "ap-south-1"
access_key_id = "AKIAUHJUOB4AZJ67BM6G"
secret_access_key = "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"
# Step1 : create aws session
session = Session(
    region_name=region
)
# Step2 : Create client of a particular service
secrets_manager_client = session.client("secretsmanager")

# Step3 : using the client perform the actions
output = get_secret("dev/rds/mysql", secrets_manager_client)
print(output)
scerets = json.loads(output)
print(scerets["username"], scerets["password"])

# aws_session = create_session("access_key_secret",access_key_id="AKIAUHJUOB4A5LHINLFT",secret_access_key="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",region = "ap-south-1")
# secretsmanager_client = create_aws_client(aws_session, "ap-south-1", aws_service_name="secretsmanager")
# paramstore_client = create_aws_client(aws_session, "ap-south-1", aws_service_name="ssm")
# print(json.loads(get_secret("dev/rds/myinfra/mysql/username_password", secretsmanager_client)))
