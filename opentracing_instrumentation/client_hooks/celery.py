from __future__ import absolute_import

import opentracing
from opentracing.ext import tags

from ..request_context import get_current_span, RequestContextManager
from ._singleton import singleton


try:
    from celery.app.task import Task
    from celery.signals import before_task_publish, task_prerun, task_postrun
except ImportError:  # pragma: no cover
    pass
else:
    _task_apply_async = Task.apply_async


def set_common_tags(span, task, span_kind):
    span.set_tag(tags.SPAN_KIND, span_kind)
    span.set_tag(tags.COMPONENT, 'Celery')
    span.set_tag('celery.task_name', task.name)


def before_task_publish_handler(headers, **kwargs):
    headers['parent_span_context'] = span_context = {}
    opentracing.tracer.inject(span_context=get_current_span().context,
                              format=opentracing.Format.TEXT_MAP,
                              carrier=span_context)


def task_prerun_handler(task, task_id, **kwargs):
    request = task.request

    operation_name = 'Celery:run:{}'.format(task.name)
    child_of = None
    if request.delivery_info.get('is_eager'):
        child_of = get_current_span()
    else:
        parent_span_context = getattr(request, 'parent_span_context', None)
        if parent_span_context:
            child_of = opentracing.tracer.extract(
                opentracing.Format.TEXT_MAP, parent_span_context
            )

    span = opentracing.tracer.start_span(operation_name=operation_name,
                                         child_of=child_of)
    set_common_tags(span, task, tags.SPAN_KIND_RPC_SERVER)
    span.set_tag('celery.task_id', task_id)

    request.tracing_context = RequestContextManager(span)
    request.tracing_context.__enter__()


def task_postrun_handler(task, **kwargs):
    get_current_span().finish()
    task.request.tracing_context.__exit__()


@singleton
def install_patches():
    if 'Task' not in globals():
        return

    def task_apply_async_wrapper(task, args=None, kwargs=None, **other_kwargs):
        operation_name = 'Celery:apply_async:{}'.format(task.name)
        span = opentracing.tracer.start_span(operation_name=operation_name,
                                             child_of=get_current_span())
        set_common_tags(span, task, tags.SPAN_KIND_RPC_CLIENT)

        with RequestContextManager(span=span), span:
            result = _task_apply_async(task, args, kwargs, **other_kwargs)
            span.set_tag('celery.task_id', result.task_id)
            return result

    before_task_publish.connect(before_task_publish_handler)
    task_prerun.connect(task_prerun_handler)
    task_postrun.connect(task_postrun_handler)

    Task.apply_async = task_apply_async_wrapper


def reset_patches():
    if 'Task' in globals():
        Task.apply_async = _task_apply_async
        before_task_publish.disconnect(before_task_publish_handler)
        task_prerun.disconnect(task_prerun_handler)
        task_postrun.disconnect(task_postrun_handler)
    install_patches.reset()
