import mock
import pytest
import requests

from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute

from opentracing.ext import tags

from opentracing_instrumentation.client_hooks import boto3 as boto3_hooks
from opentracing_instrumentation.request_context import span_in_context


SKIP_REASON = 'DynamoDB is not running or cannot connect'
DYNAMODB_ENDPOINT_URL = 'http://localhost:8000'


class UserModel(Model):

    class Meta:
        table_name = 'users'
        host = DYNAMODB_ENDPOINT_URL
        aws_access_key_id = '-'
        aws_secret_access_key = '-'
        read_capacity_units = 10
        write_capacity_units = 10

    username = UnicodeAttribute(hash_key=True)
    first_name = UnicodeAttribute()
    last_name = UnicodeAttribute()


@pytest.fixture(scope='module')
def test_user():
    UserModel.create_table()
    user = UserModel(username='janedoe',
                     first_name='Jane',
                     last_name='Doe')
    user.save()

    try:
        yield user
    finally:
        user.delete()


@pytest.fixture
def patch_boto3():
    boto3_hooks.install_patches()
    try:
        yield
    finally:
        boto3_hooks.reset_patches()


def is_dynamodb_running():
    try:
        # feel free to propose better solution for this check
        response = requests.get(DYNAMODB_ENDPOINT_URL, timeout=1)
        return response.status_code == 400
    except requests.exceptions.ConnectionError:
        return False


@pytest.mark.skipif(not is_dynamodb_running(), reason=SKIP_REASON)
def test_boto3(test_user, tracer, patch_boto3):
    # TODO: get some data from DynamoDB

    # span = tracer.recorder.get_spans()[0]
    # assert span.tags.get(tags.SPAN_KIND) == tags.SPAN_KIND_RPC_CLIENT
    # assert span.tags.get(tags.COMPONENT) == 'boto3'
    # assert span.tags.get('boto3.service_name') == 'dynamodb'
    pass
