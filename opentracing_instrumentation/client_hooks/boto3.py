from __future__ import absolute_import

from opentracing.ext import tags

from opentracing_instrumentation import utils
from ..request_context import get_current_span
from ._singleton import singleton


try:
    from boto3.resources.action import ServiceAction
    from botocore import xform_name
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    pass
else:
    _service_action_call = ServiceAction.__call__


@singleton
def install_patches():
    if '_service_action_call' not in globals():
        return

    def set_request_id_tag(span, response):
        metadata = response.get('ResponseMetadata')

        # there is no ResponseMetadata for
        # boto3:dynamodb:describe_table
        if metadata:
            span.set_tag('aws.request_id', metadata['RequestId'])

    def call_wrapper(self, parent, *args, **kwargs):
        """Wraps ServiceAction.__call__"""

        service_name = parent.meta.service_name
        operation_name = 'boto3:{}:{}'.format(
            service_name, xform_name(self._action_model.request.operation)
        )
        span = utils.start_child_span(operation_name=operation_name,
                                      parent=get_current_span())

        span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
        span.set_tag(tags.COMPONENT, 'boto3')
        span.set_tag('boto3.service_name', service_name)

        with span:
            try:
                response = _service_action_call(self, parent, *args, **kwargs)
            except ClientError as error:
                set_request_id_tag(span, error.response)
                raise
            else:
                if isinstance(response, dict):
                    set_request_id_tag(span, response)

        return response

    ServiceAction.__call__ = call_wrapper


def reset_patches():
    if '_service_action_call' in globals():
        ServiceAction.__call__ = _service_action_call
    install_patches.reset()
