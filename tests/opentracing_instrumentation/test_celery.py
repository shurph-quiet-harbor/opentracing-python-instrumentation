import celery as celery_module
import mock
import pytest

from celery import Celery
from celery.signals import (
    after_task_publish, before_task_publish, task_prerun, task_postrun
)
from distutils.version import LooseVersion
from kombu import Connection
from opentracing.ext import tags

from opentracing_instrumentation.client_hooks import celery as celery_hooks


@pytest.fixture(autouse=True, scope='module')
def patch_celery():
    celery_hooks.install_patches()
    try:
        yield
    finally:
        celery_hooks.reset_patches()


def assert_span(span, result, operation, span_kind):
    assert span.operation_name == 'Celery:{}:foo'.format(operation)
    assert span.tags.get(tags.SPAN_KIND) == span_kind
    assert span.tags.get(tags.COMPONENT) == 'Celery'
    assert span.tags.get('celery.task_name') == 'foo'
    assert span.tags.get('celery.task_id') == result.task_id


def _test_foo_task(celery):

    @celery.task(name='foo')
    def foo():
        foo.called = True
    foo.called = False

    result = foo.delay()
    assert foo.called

    return result


def _test(celery, tracer):
    result = _test_foo_task(celery)

    span_server, span_client = tracer.recorder.get_spans()
    assert span_client.parent_id is None
    assert span_client.context.trace_id == span_server.context.trace_id
    assert span_client.context.span_id == span_server.parent_id

    assert_span(span_client, result, 'apply_async', tags.SPAN_KIND_RPC_CLIENT)
    assert_span(span_server, result, 'run', tags.SPAN_KIND_RPC_SERVER)


def is_rabbitmq_running():
    try:
        Connection('amqp://guest:guest@127.0.0.1:5672//').connect()
        return True
    except:
        return False


@pytest.mark.skipif(not is_rabbitmq_running(),
                    reason='RabbitMQ is not running or cannot connect')
def test_celery_with_rabbitmq(tracer):
    celery = Celery('test')

    @after_task_publish.connect
    def run_worker(**kwargs):
        worker = celery.Worker(concurrency=1,
                               pool_cls='solo',
                               use_eventloop=False,
                               prefetch_multiplier=1,
                               quiet=True)

        @task_postrun.connect
        def stop_worker_soon(**kwargs):
            if LooseVersion(celery_module.__version__) >= LooseVersion('4'):
                def stop_worker():
                    # avoiding AttributeError that makes tests noisy
                    worker.consumer.connection.drain_events = mock.Mock()

                    worker.stop()

                # worker must be stopped not earlier than
                # data exchange with RabbitMQ is completed
                worker.consumer._pending_operations.insert(0, stop_worker)
            else:
                worker.stop()

        worker.start()

    _test(celery, tracer)


@pytest.fixture
def celery_eager():
    celery = Celery('test')
    celery.config_from_object({
        'task_always_eager': True,  # Celery 4.x
        'CELERY_ALWAYS_EAGER': True,  # Celery 3.x
    })
    return celery


def test_celery_eager(celery_eager, tracer):
    _test(celery_eager, tracer)


def test_celery_run_without_parent_span(celery_eager, tracer):

    def task_prerun_hook(task, **kwargs):
        task.request.delivery_info['is_eager'] = False

    task_prerun.connect(task_prerun_hook)
    task_prerun.receivers = list(reversed(task_prerun.receivers))
    try:
        result = _test_foo_task(celery_eager)
    finally:
        task_prerun.disconnect(task_prerun_hook)

    span_server = tracer.recorder.get_spans()[0]
    assert span_server.parent_id is None
    assert_span(span_server, result, 'run', tags.SPAN_KIND_RPC_SERVER)


@mock.patch.object(celery_hooks, 'Task')
def test_celery_patching(*mocks):
    celery_hooks.reset_patches()
    del celery_hooks.Task
    celery_hooks.install_patches()

    assert not before_task_publish.receivers
    assert not task_prerun.receivers
    assert not task_postrun.receivers

    celery_hooks.reset_patches()
