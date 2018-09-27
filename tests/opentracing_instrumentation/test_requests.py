import mock
import pytest
import requests


from opentracing_instrumentation.client_hooks import requests as requests_hooks
from opentracing_instrumentation.request_context import span_in_context


@pytest.fixture
def patch_requests(request):
    if request.getfixturevalue('hook'):
        response_handler_hook = mock.Mock()
    else:
        response_handler_hook = None

    requests_hooks.install_patches(response_handler_hook)
    try:
        yield response_handler_hook
    finally:
        requests_hooks.reset_patches()


@pytest.mark.parametrize('scheme,root_span,hook', [
    ('http', True, True),
    ('http', True, False),
    ('http', False, True),
    ('http', False, False),
    ('https', True, True),
    ('https', True, False),
    ('https', False, True),
    ('https', False, False),
])
@mock.patch('requests.adapters.HTTPAdapter.cert_verify')
@mock.patch('requests.adapters.HTTPAdapter.get_connection')
def test_requests(get_connection_mock, cert_verify_mock,
                  scheme, root_span, hook, tracer, patch_requests):
    url = '{}://example.com/'.format(scheme)

    if root_span:
        root_span = tracer.start_span('root-span')
    else:
        root_span = None

    with span_in_context(span=root_span):
        response = requests.get(url)

    assert len(tracer.recorder.get_spans()) == 1

    span = tracer.recorder.get_spans()[0]
    assert span.tags.get('span.kind') == 'client'
    assert span.tags.get('http.url') == url

    # verify trace-id was correctly injected into headers
    headers = get_connection_mock.return_value.urlopen.call_args[1]['headers']
    trace_id = headers.get('ot-tracer-traceid')
    assert trace_id == '%x' % span.context.trace_id

    if hook:
        patch_requests.assert_called_once_with(response, span)
