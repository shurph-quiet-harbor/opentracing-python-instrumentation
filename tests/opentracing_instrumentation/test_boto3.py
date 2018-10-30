import boto3
import pytest
import requests

from botocore.exceptions import ClientError
from opentracing.ext import tags

from opentracing_instrumentation.client_hooks import boto3 as boto3_hooks


SKIP_REASON = 'DynamoDB is not running or cannot connect'
DYNAMODB_ENDPOINT_URL = 'http://localhost:8000'

AWS_CONFIG = {
    'endpoint_url': DYNAMODB_ENDPOINT_URL,
    'aws_access_key_id': '-',
    'aws_secret_access_key': '-',
    'region_name': 'us-east-1',
}


@pytest.fixture(scope='module')
def dynamodb():
    dynamodb = boto3.resource('dynamodb', **AWS_CONFIG)

    try:
        dynamodb.Table('users').delete()
    except ClientError as error:
        # you can not just use ResourceNotFoundException class
        # to catch an error since it doesn't exist until it's raised
        if error.__class__.__name__ != 'ResourceNotFoundException':
            raise

    table = dynamodb.create_table(
        TableName='users',
        KeySchema=[{
            'AttributeName': 'username',
            'KeyType': 'HASH'
        }],
        AttributeDefinitions=[{
            'AttributeName': 'username',
            'AttributeType': 'S'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 9,
            'WriteCapacityUnits': 9
        }
    )

    # waiting until the table exists
    table.meta.client.get_waiter('table_exists').wait(TableName='users')

    return dynamodb


@pytest.fixture
def patch_boto3():
    boto3_hooks.install_patches()
    try:
        yield
    finally:
        boto3_hooks.reset_patches()


def is_dynamodb_running():
    try:
        # feel free to suggest better solution for this check
        response = requests.get(DYNAMODB_ENDPOINT_URL, timeout=1)
        return response.status_code == 400
    except requests.exceptions.ConnectionError:
        return False


@pytest.mark.skipif(not is_dynamodb_running(), reason=SKIP_REASON)
def test_boto3(dynamodb, tracer, patch_boto3):

    def assert_last_span(operation):
        span = tracer.recorder.get_spans()[-1]
        request_id = response['ResponseMetadata']['RequestId']
        assert span.operation_name == 'boto3:dynamodb:' + operation
        assert span.tags.get(tags.SPAN_KIND) == tags.SPAN_KIND_RPC_CLIENT
        assert span.tags.get(tags.COMPONENT) == 'boto3'
        assert span.tags.get('boto3.service_name') == 'dynamodb'
        assert span.tags.get('aws.request_id') == request_id

    users = dynamodb.Table('users')

    response = users.put_item(Item={
        'username': 'janedoe',
        'first_name': 'Jane',
        'last_name': 'Doe',
    })
    assert_last_span('put_item')

    response = users.get_item(Key={'username': 'janedoe'})
    user = response['Item']
    assert user['first_name'] == 'Jane'
    assert user['last_name'] == 'Doe'
    assert_last_span('get_item')

    try:
        dynamodb.Table('test').delete_item(Key={'username': 'janedoe'})
    except ClientError as error:
        response = error.response
    assert_last_span('delete_item')
