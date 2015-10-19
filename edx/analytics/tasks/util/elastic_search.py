import time
from urllib import urlencode
try:
    from boto.connection import AWSAuthConnection
except ImportError:
    AWSAuthConnection = object

try:
    from elasticsearch import Connection
    from elasticsearch import ImproperlyConfigured
except ImportError:
    Connection = object


class BotoHttpConnection(Connection):

    def __init__(self, host='localhost', **kwargs):
        super(BotoHttpConnection, self).__init__(host=host, port=443, **kwargs)
        other_args = {}
        if 'timeout' in kwargs:
            other_args['timeout'] = kwargs['timeout']
        self.connection = ESConnection(host=host, port=443, **other_args)

    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=()):
        if params:
            url = '%s?%s' % (url, urlencode(params or {}))

        start = time.time()
        response = self.connection.make_request(method, url, data=body)
        duration = time.time() - start
        raw_data = response.read()

        # raise errors based on http status codes, let the client handle those if needed
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(method, url, body, duration, response.status)
            self._raise_error(response.status_code, raw_data)

        self.log_request_success(method, url, url, body, response.status, raw_data, duration)

        return response.status, response.getheaders(), raw_data


class ESConnection(AWSAuthConnection):

    def __init__(self, **kwargs):
        kwargs['is_secure'] = True
        super(ESConnection, self).__init__(**kwargs)
        #self._set_auth_region_name(region)
        self._set_auth_service_name("es")

    def _required_auth_capability(self):
        return ['hmac-v4']