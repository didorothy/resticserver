import datetime
import functools
import http
import io
import json
import os
import os.path
import re
import shutil
import stat
import sys
import traceback
import urllib.parse

import config

READ_BYTES = 8192

RESTIC_TYPES = [
    'data',
    'keys',
    'locks',
    'snapshots',
    'index',
    'config',
]

PATH_RE = re.compile(r'^/?(?P<path>[^/]+)/?(?P<type>data|keys|locks|snapshots|index|config)?/?(?P<name>[^/]+)?/?$')
RANGE_RE = re.compile(r'^bytes=(?P<start>\d+)-(?P<end>\d+)?')

HTTP_RESPONSES = {
    100: '100 CONTINUE',
    101: '101 SWITCHING_PROTOCOLS',
    102: '102 PROCESSING',
    200: '200 OK',
    201: '201 CREATED',
    202: '202 ACCEPTED',
    203: '203 NON_AUTHORITATIVE_INFORMATION',
    204: '204 NO_CONTENT',
    205: '205 RESET_CONTENT',
    206: '206 PARTIAL_CONTENT',
    207: '207 MULTI_STATUS',
    208: '208 ALREADY_REPORTED',
    226: '226 IM_USED',
    300: '300 MULTIPLE_CHOICES',
    301: '301 MOVED_PERMANENTLY',
    302: '302 FOUND',
    303: '303 SEE_OTHER',
    304: '304 NOT_MODIFIED',
    305: '305 USE_PROXY',
    307: '307 TEMPORARY_REDIRECT',
    308: '308 PERMANENT_REDIRECT',
    400: '400 BAD_REQUEST',
    401: '401 UNAUTHORIZED',
    402: '402 PAYMENT_REQUIRED',
    403: '403 FORBIDDEN',
    404: '404 NOT_FOUND',
    405: '405 METHOD_NOT_ALLOWED',
    406: '406 NOT_ACCEPTABLE',
    407: '407 PROXY_AUTHENTICATION_REQUIRED',
    408: '408 REQUEST_TIMEOUT',
    409: '409 CONFLICT',
    410: '410 GONE',
    411: '411 LENGTH_REQUIRED',
    412: '412 PRECONDITION_FAILED',
    413: '413 REQUEST_ENTITY_TOO_LARGE',
    414: '414 REQUEST_URI_TOO_LONG',
    415: '415 UNSUPPORTED_MEDIA_TYPE',
    416: '416 REQUESTED_RANGE_NOT_SATISFIABLE',
    417: '417 EXPECTATION_FAILED',
    421: '421 MISDIRECTED_REQUEST',
    422: '422 UNPROCESSABLE_ENTITY',
    423: '423 LOCKED',
    424: '424 FAILED_DEPENDENCY',
    426: '426 UPGRADE_REQUIRED',
    428: '428 PRECONDITION_REQUIRED',
    429: '429 TOO_MANY_REQUESTS',
    431: '431 REQUEST_HEADER_FIELDS_TOO_LARGE',
    500: '500 INTERNAL_SERVER_ERROR',
    501: '501 NOT_IMPLEMENTED',
    502: '502 BAD_GATEWAY',
    503: '503 SERVICE_UNAVAILABLE',
    504: '504 GATEWAY_TIMEOUT',
    505: '505 HTTP_VERSION_NOT_SUPPORTED',
    506: '506 VARIANT_ALSO_NEGOTIATES',
    507: '507 INSUFFICIENT_STORAGE',
    508: '508 LOOP_DETECTED',
    510: '510 NOT_EXTENDED',
    511: '511 NETWORK_AUTHENTICATION_REQUIRED',
}

def valid_methods(methods):
    '''Decorator to ensure that only specific methods are allowed.'''
    if isinstance(methods, str):
        methods = [methods.upper()]
    else:
        methods = [m.upper() for m in methods]
    def decorator_method(func):

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.environ['REQUEST_METHOD'] not in methods:
                raise Exception('Method not allowed')
            return func(self, *args, **kwargs)

        return wrapper

    return decorator_method


class Application:
    '''The Restic Rest Server implementation.'''

    def __init__(self, environ, start_response):
        self.environ = environ
        self._start_response = start_response
        self.ROOT_PATH = config.ROOT_PATH

    def start_response(self, status, headers):
        self.log('start_response called.')
        return self._start_response(status, headers)

    def __iter__(self):
        try:
            self.log('Starting processing (method: {})'.format(self.environ['REQUEST_METHOD']))
            path, restic_type, name = self.get_path()
            GET_DICT = urllib.parse.parse_qs(self.environ['QUERY_STRING'])
            self.log(self.environ)
            create = True if 'true' in GET_DICT.get('create', []) else False
            self.log('Create: {}'.format(create))
            self.log('{} {} {}'.format(path, restic_type, name))

            if self.environ['REQUEST_METHOD'] == 'GET':
                if path is not None and restic_type == 'config':
                    return self.get_config(path)

                elif path is not None and restic_type is not None and name is None:
                    return self.get_path_list(path, restic_type)

                elif path is not None and restic_type is not None and name is not None:
                    return self.get_path_data(path, restic_type, name)

                else:
                    return self.yield_error('Not Implemented')
            elif self.environ['REQUEST_METHOD'] == 'POST':
                if create and restic_type is None and name is None:
                    return self.create_repository(path)

                elif path is not None and restic_type == 'config':
                    return self.set_config(path)

                elif path is not None and restic_type is not None and name is not None:
                    return self.set_path_data(path, restic_type, name)

                else:
                    return self.yield_error('Not Implemented')
            elif self.environ['REQUEST_METHOD'] == 'DELETE':
                if path is not None and restic_type is None and name is None:
                    return self.delete_repository(path)

                elif path is not None and restic_type is not None and name is not None:
                    return self.delete_path_data(path, restic_type, name)

                else:
                    return self.yield_error('Not Implemented')
            elif self.environ['REQUEST_METHOD'] == 'HEAD':
                if path is not None and restic_type == 'config':
                    return self.config_exists(path)

                if path is not None and restic_type is not None and name is not None:
                    return self.get_path_check(path, restic_type, name)

                else:
                    return self.yield_error('Not Implemented')
            else:
                return self.yield_error('Unsupported request method.')
        except Exception as ex:
            self.log(str(ex))
            self.stack_trace(ex)
            return self.yield_error('Something unexpected occurred.')

    def log(self, message):
        '''Writes a message to the error log.'''
        out_stream = self.environ['wsgi.errors']
        out_stream.write('{} {}\n'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            message
        ))

    def stack_trace(self, ex):
        '''Writes a stack trace to the error log.'''
        out_stream = self.environ['wsgi.errors']
        exc_type, exc_value, exc_traceback = sys.exc_info()
        for line in traceback.format_tb(exc_traceback):
            out_stream.write(line)

    def get_path(self):
        '''Parses PATH_INFO and returns the path, the type if any, and the name if any.'''
        match = PATH_RE.match(self.environ['PATH_INFO'])
        if match:
            path = match.group('path')
            restic_type = match.group('type')
            name = match.group('name')
            return path, restic_type, name
        else:
            return None, None, None

    def send_error(self, message):
        '''Sends a generic error message.'''
        self.log('Error: {}'.format(message))
        self.start_response(HTTP_RESPONSES[500], [('Content-Type', 'text/plain')])
        return message.encode('utf-8')

    def yield_error(self, message):
        self.log('Error: {}'.format(message))
        self.start_response(HTTP_RESPONSES[500], [('Content-Type', 'text/plain')])
        yield message.encode('utf-8')

    def send_not_found(self, message):
        '''Sends a generic not found message.'''
        self.log('Not Found: {}'.format(message))
        self.start_response(HTTP_RESPONSES[404], [('Content-Type', 'text/plain')])
        return message.encode('utf-8')

    @valid_methods('POST')
    def create_repository(self, path):
        '''This request is used to initially create a new repository. The
        server responds with "200 OK" if the repository structure was created
        successfully or already exists, otherwise an error is returned.
        '''
        self.log('got to create_repository')
        full_path = os.path.join(self.ROOT_PATH, path)
        if not os.path.exists(full_path):
            try:
                os.mkdir(full_path)
                for restic_type in RESTIC_TYPES:
                    if restic_type != 'config':
                        os.mkdir(os.path.join(full_path, restic_type))
            except:
                self.log('Got here...')
                yield self.send_error('Failed to create repository.')

        self.start_response(HTTP_RESPONSES[200], [])
        yield b''

    @valid_methods('DELETE')
    def delete_repository(self, path):
        '''Deletes the repository on the server side. The server responds
        with "200 OK" if the repository was successfully removed. If this
        function is not implemented the server returns "501 Not Implemented",
        if this it is denied by the server it returns "403 Forbidden".
        '''
        full_path = os.path.join(self.ROOT_PATH, path)
        if os.path.exists(full_path):
            shutil.rmtree(full_path)
            self.start_response(HTTP_RESPONSES[200], [])
            yield b''
        else:
            yield self.send_error('Repository does not exist.')

    @valid_methods('HEAD')
    def config_exists(self, path):
        '''Returns 200 OK if the repository has a configuration, an HTTP error
        otherwise.
        '''
        self.log('checking if config exists')

        full_path = os.path.join(self.ROOT_PATH, path, 'config')
        self.log(full_path)
        if os.path.exists(full_path):
            stats = os.stat(full_path)
            self.start_response(HTTP_RESPONSES[200], [
                ('Content-Type', 'text/plain'),
                ('Content-Length', str(stats.st_size))
            ])
            yield b''
        else:
            self.log('"{}" config does not exist'.format(path))
            yield self.send_error('Configuration does not exist.')

    @valid_methods('GET')
    def get_config(self, path):
        '''Returns the content of the configuration file if the repository has
        a configuration, an HTTP error otherwise.
        '''
        self.log('retrieving config')

        full_path = os.path.join(self.ROOT_PATH, path, 'config')
        if os.path.exists(full_path):
            stats = os.stat(full_path)
            self.start_response(HTTP_RESPONSES[200], [
                ('Content-Type', 'binary/octet-stream'),
                ('Content-Length', str(stats.st_size))
            ])
            with open(full_path, 'rb') as in_file:
                while True:
                    data = in_file.read(READ_BYTES)
                    if not data:
                        break
                    yield data
        else:
            yield self.send_error('Configuration does not exist.')

    @valid_methods('POST')
    def set_config(self, path):
        self.log('saving config')

        full_path = os.path.join(self.ROOT_PATH, path, 'config')

        with open(full_path, 'wb') as out_file:
            while True:
                data = self.environ['wsgi.input'].read(READ_BYTES)
                if not data:
                    break
                out_file.write(data)
        self.start_response(HTTP_RESPONSES[200], [])
        yield b''

    @valid_methods('GET')
    def get_path_list(self, path, restic_type):
        '''Returns a JSON array containing an object for each file of the
        given type. The objects have two keys: name for the file name,
        and size for the size in bytes.
        '''
        full_path = os.path.join(self.ROOT_PATH, path, restic_type)
        results = []
        if restic_type == 'data':
            for folder in os.listdir(full_path):
                folder_path = os.path.join(full_path, folder)
                for name in os.listdir(folder_path):
                    stats = os.stat(os.path.join(folder_path, name))
                    if stat.S_ISREG(stats.st_mode):
                        results.append({
                            'name': name,
                            'size': stats.st_size,
                        })
            pass
        else:
            for name in os.listdir(full_path):
                stats = os.stat(os.path.join(full_path, name))
                if stat.S_ISREG(stats.st_mode):
                    results.append({
                        'name': name,
                        'size': stats.st_size,
                    })

        self.start_response(HTTP_RESPONSES[200], [
            ('Content-Type', 'application/vnd.x.restic.rest.v2')
        ])
        yield json.dumps(results).encode('utf-8')

    @valid_methods('HEAD')
    def get_path_check(self, path, restic_type, name):
        '''Returns "200 OK" if the blob with the given name and type is stored
        in the repository, "404 not found" otherwise. If the blob exists, the
        HTTP header Content-Length is set to the file size.
        '''
        if restic_type == 'data':
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name[:2], name)
        else:
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name)

        if os.path.exists(full_path):
            stats = os.stat(full_path)
            self.start_response(HTTP_RESPONSES[200], [
                ('Content-Type', 'text/plain'),
                ('Content-Length', str(stats.st_size))
            ])
            yield b''
        else:
            yield self.send_not_found('Requested path data does not exist.')

    @valid_methods('GET')
    def get_path_data(self, path, restic_type, name):
        '''Returns the content of the blob with the given name and type if it
        is stored in the repository, "404 not found" otherwise.

        If the request specifies a partial read with a Range header field,
        then the status code of the response is 206 instead of 200 and the
        response only contains the specified range.

        Response format: binary/octet-stream
        '''
        if restic_type == 'data':
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name[:2], name)
        else:
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name)

        if os.path.exists(full_path):
            stats = os.stat(full_path)

            http_range = self.environ.get('HTTP_RANGE')
            self.log('HTTP Range: {}'.format(http_range))
            range_match = None
            if http_range:
                range_match = RANGE_RE.match(http_range)
            if http_range and range_match:
                start = int(range_match.group('start'))
                end = range_match.group('end')
                if end:
                    end = int(end)
                length = end - start + 1 if end else stats.st_size - start

                self.log('{} - {} : {}'.format(start, end, length))

                self.start_response(HTTP_RESPONSES[206], [
                    ('Content-Type', 'binary/octet-stream'),
                    ('Content-Length', str(length)),
                    ('Content-Range', 'bytes={}-{}'.format(start, end or ''))
                ])

                with open(full_path, 'rb') as in_file:
                    in_file.seek(start)
                    sent_length = 0
                    while True:
                        data = in_file.read(READ_BYTES)
                        if not data or sent_length >= length:
                            break

                        if sent_length + len(data) > length:
                            yield data[:length - sent_length]
                        else:
                            yield data
                        sent_length += len(data)
            else:
                self.start_response(HTTP_RESPONSES[200], [
                    ('Content-Type', 'binary/octet-stream'),
                    ('Content-Length', str(stats.st_size))
                ])
                with open(full_path, 'rb') as in_file:
                    while True:
                        data = in_file.read(READ_BYTES)
                        if not data:
                            break
                        yield data
        else:
            yield self.send_not_found('Requested path data does not exist.')

    @valid_methods('POST')
    def set_path_data(self, path, restic_type, name):
        '''Saves the content of the request body as a blob with the given name
        and type, an HTTP error otherwise.

        Request format: binary/octet-stream
        '''
        if restic_type == 'data':
            if not os.path.exists(os.path.join(self.ROOT_PATH, path, restic_type, name[:2])):
                os.mkdir(os.path.join(self.ROOT_PATH, path, restic_type, name[:2]))
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name[:2], name)
        else:
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name)
        with open(full_path, 'wb') as out_file:
            while True:
                data = self.environ['wsgi.input'].read(READ_BYTES)
                if not data:
                    break
                out_file.write(data)
        self.start_response(HTTP_RESPONSES[200], [])
        yield b''

    @valid_methods('DELETE')
    def delete_path_data(self, path, restic_type, name):
        '''Returns "200 OK" if the blob with the given name and type has been
        deleted from the repository, an HTTP error otherwise.
        '''
        if restic_type == 'data':
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name[:2], name)
        else:
            full_path = os.path.join(self.ROOT_PATH, path, restic_type, name)

        if os.path.exists(full_path):
            os.unlink(full_path)
            self.start_response(HTTP_RESPONSES[200], [])
            yield b''
        else:
            yield self.send_error('Path data does not exist.')


if __name__ == '__main__':
    #from wsgiref.simple_server import make_server

    #httpd = make_server('', 9000, Application)
    print('Serving HTTP on port 9000...')

    #httpd.serve_forever()
    from waitress import serve
    serve(Application, listen='0.0.0.0:9000')
