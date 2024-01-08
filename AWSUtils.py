# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/
import traceback

import base64

from boto3 import Session
from botocore.exceptions import ClientError
import awswrangler as wr


def create_session(approach: str, access_key_id="", secret_access_key="", region="") -> Session:
    try:
        print(f"Trying to create AWS session, Chosen Approach:{approach}")
        if approach == "access_key_secret":
            session = Session(
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
            print(f"Session is successfully created")
            return session
        elif approach == "role":
            session = Session(
                region_name=region
            )
            print(f"Session is successfully created")
            return session
        else:
            print("Unable to find the approach to create aws session it should be either access_key_secret or "
                  "role")
    except Exception as ex:
        print("Unable to create s3 connect due to below error")
        print(traceback.print_exc())
        raise ex


def get_aws_session(approach: str, aws_security_config):
    if approach == "access_key_secret":
        return create_session("access_key_secret", access_key_id=aws_security_config["access_key_id"],
                              secret_access_key=aws_security_config["secret_access_key"],
                              region=aws_security_config["region"])
    if approach == "role":
        return create_session("access_key_secret", region=aws_security_config["region"])


def create_aws_client(session, region_name, aws_service_name=""):
    # secretsmanager
    try:
        client = session.client(
            service_name=aws_service_name,
            region_name=region_name
        )
        return client
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e


def get_secret(secret_name, sm_client):
    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
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
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


def get_parameter(parameter, ssm_client):
    try:
        ssm_response = ssm_client.get_parameter(
            Name=parameter,
            WithDecryption=True
        )
    except ClientError as e:
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
        return ssm_response.get("Parameter").get("Value")


def write_to_s3_update_catalog(df, boto3_session, s3_dataset_prefix, glue_catalog_database, glue_catalog_tablename,
                               partition_columns):
    # https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.s3.to_parquet.html
    wr.catalog.create_database(
        name=glue_catalog_database,
        exist_ok=True,
        boto3_session=boto3_session
    )
    if len(partition_columns) > 0:
        wr.s3.to_parquet(
            df=df,
            path=s3_dataset_prefix,
            dataset=True,
            partition_cols=partition_columns,
            database=glue_catalog_database,  # Athena/Glue database
            table=glue_catalog_tablename,  # Athena/Glue table
            boto3_session=boto3_session,
            mode="append"
        )
    else:
        wr.s3.to_parquet(
            df=df,
            path=s3_dataset_prefix,
            dataset=True,
            database=glue_catalog_database,  # Athena/Glue database
            table=glue_catalog_tablename,  # Athena/Glue table
            boto3_session=boto3_session,
            mode="overwrite"
        )


def read_from_athena(boto3_session,query, glue_database_name):
    return wr.athena.read_sql_query(
        sql=query,
        database=glue_database_name,
        boto3_session= boto3_session
    )


def create_s3_bucket_if_not_exist(s3_client, region, bucket_name):
    try:
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        # print('Done!')
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            return True
        else:
            print(traceback.format_exc())
    else:
        return True
