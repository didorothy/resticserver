import io
import json
import os
import os.path
import shutil
import types
import unittest

import resticserver


class Response:
    '''Fake start_response object to allow calls without actually doing anything.'''
    def __init__(self):
        self.status = None
        self.headers = None
        self.exec_info = None

    def __call__(self, status, headers, exec_info=None):
        self.status = status
        self.headers = headers
        self.exec_info = exec_info


class TestApplication(unittest.TestCase):

    def create_application(self, env, response):
        '''Creates the applicaiton with a test root path and ensures that the directory exists.'''
        app = resticserver.Application(env, response)
        exec_path = os.path.abspath(os.path.dirname(__file__))
        app.ROOT_PATH = os.path.join(exec_path, 'test_backups')
        if not os.path.exists(app.ROOT_PATH):
            os.mkdir(app.ROOT_PATH)
        return app

    def ensure_directory(self, path):
        '''Ensures that a directory exists.'''
        if not os.path.exists(path):
            os.makedirs(path)

    def ensure_file(self, path, contents:bytes):
        '''Ensure that a file exists with the specified contents.

        Creates directories if needed.
        '''
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        with open(path, 'wb') as out_file:
            out_file.write(contents)

    def test_send_error(self):
        env = {
            'PATH_INFO': '/testrepo',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        result = app.send_error('Test Message')
        self.assertIsInstance(result, bytes)
        self.assertEqual(response.status, resticserver.HTTP_RESPONSES[500])
        self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
        self.assertEqual(result.decode('utf8'), 'Test Message')

    def test_send_not_found(self):
        env = {
            'PATH_INFO': '/testrepo',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        result = app.send_not_found('Test Message')
        self.assertIsInstance(result, bytes)
        self.assertEqual(response.status, resticserver.HTTP_RESPONSES[404])
        self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
        self.assertEqual(result.decode('utf8'), 'Test Message')

    def test_create_repository(self):
        '''Ensures that we can create a test repository.'''
        env = {
            'PATH_INFO': '/testrepo',
            'QUERY_STRING': 'create=true',
            'REQUEST_METHOD': 'POST',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        # ensure that the repository is not yet created.
        if os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))
        result = b''.join([x for x in app])

        try:

            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            self.assertTrue(os.path.isdir(os.path.join(app.ROOT_PATH, 'testrepo')))
            for t in resticserver.RESTIC_TYPES:
                if t != 'config':
                    self.assertTrue(os.path.isdir(os.path.join(app.ROOT_PATH, 'testrepo', t)))
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_delete_repository(self):
        '''Ensure that we can delete a test repository.'''
        env = {
            'PATH_INFO': '/testrepo',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'DELETE',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        # ensure that the repository exists
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            self.assertFalse(os.path.isdir(os.path.join(app.ROOT_PATH, 'testrepo')))
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_delete_repository_nonexisting(self):
        env = {
            'PATH_INFO': '/nonexistrepo',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'DELETE',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[500])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf8'), 'Repository does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_config_exists(self):
        env = {
            'PATH_INFO': '/testrepo/config',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        with open(os.path.join(app.ROOT_PATH, 'testrepo', 'config'), 'wb') as out_file:
            out_file.write(b'This is the config...')
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'text/plain'),
                ('Content-Length', '21')
            ])
            self.assertEqual(result.decode('utf8'), '')
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_config_exists_not_exists(self):
        env = {
            'PATH_INFO': '/testrepo/config',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        if os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'config')):
            os.unlink(os.path.join(app.ROOT_PATH, 'testrepo', 'config'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[500])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf8'), 'Configuration does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_get_config(self):
        env = {
            'PATH_INFO': '/testrepo/config',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        with open(os.path.join(app.ROOT_PATH, 'testrepo', 'config'), 'wb') as out_file:
            out_file.write(b'This is the config...')
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'binary/octet-stream'),
                ('Content-Length', '21')
            ])
            self.assertEqual(result, b'This is the config...')
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_get_config_notexist(self):
        env = {
            'PATH_INFO': '/testrepo/config',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        if os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'config')):
            os.unlink(os.path.join(app.ROOT_PATH, 'testrepo', 'config'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[500])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf8'), 'Configuration does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_set_config(self):
        env = {
            'PATH_INFO': '/testrepo/config',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'POST',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO('{"version": 0, "status": "this is a test"}'.encode('utf8')),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        if os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'config')):
            os.unlink(os.path.join(app.ROOT_PATH, 'testrepo', 'config'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            with open(os.path.join(app.ROOT_PATH, 'testrepo', 'config'), 'rb') as infile:
                self.assertEqual(infile.read(), '{"version": 0, "status": "this is a test"}'.encode('utf8'))
        except:
            print(env['wsgi.errors'].getvalue())
            raise

    def test_get_path_list_data_empty(self):
        env = {
            'PATH_INFO': '/testrepo/data',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo'))
        if not os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'data')):
            os.mkdir(os.path.join(app.ROOT_PATH, 'testrepo', 'data'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [('Content-Type', 'application/vnd.x.restic.rest.v2')])
            self.assertEqual(json.loads(result.decode('utf-8')), [])
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_list_data_some(self):
        env = {
            'PATH_INFO': '/testrepo/data',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa00000000000000'),
            'Some test data.'.encode('utf-8')
        )
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa11111111111111'),
            'Some test data.'.encode('utf-8')
        )
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'bb', 'bb00000000000000'),
            'Some test data.'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [('Content-Type', 'application/vnd.x.restic.rest.v2')])
            self.assertEqual(json.loads(result.decode('utf-8')), [
                {
                    'name': 'aa00000000000000',
                    'size': len('Some test data.'.encode('utf-8'))
                },
                {
                    'name': 'aa11111111111111',
                    'size': len('Some test data.'.encode('utf-8'))
                },
                {
                    'name': 'bb00000000000000',
                    'size': len('Some test data.'.encode('utf-8'))
                }
            ])
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_list_other_empty(self):
        env = {
            'PATH_INFO': '/testrepo/keys',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(os.path.join(app.ROOT_PATH, 'testrepo', 'keys'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [('Content-Type', 'application/vnd.x.restic.rest.v2')])
            self.assertEqual(json.loads(result.decode('utf-8')), [])
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_list_other_some(self):
        env = {
            'PATH_INFO': '/testrepo/keys',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa00000000000000'),
            'Some test data.'.encode('utf-8')
        )
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa11111111111111'),
            'Some test data.'.encode('utf-8')
        )
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'bb00000000000000'),
            'Some test data.'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [('Content-Type', 'application/vnd.x.restic.rest.v2')])
            self.assertEqual(json.loads(result.decode('utf-8')), [
                {
                    'name': 'aa00000000000000',
                    'size': len('Some test data.'.encode('utf-8'))
                },
                {
                    'name': 'aa11111111111111',
                    'size': len('Some test data.'.encode('utf-8'))
                },
                {
                    'name': 'bb00000000000000',
                    'size': len('Some test data.'.encode('utf-8'))
                }
            ])
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_check_data_nonexist(self):
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(os.path.join(app.ROOT_PATH, 'testrepo', 'data'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[404])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf-8'), 'Requested path data does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_check_data_exist(self):
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'text/plain'),
                ('Content-Length', str(len('Test contents...'.encode('utf-8'))))
            ])
            self.assertEqual(result.decode('utf-8'), '')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_check_other_nonexist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(os.path.join(app.ROOT_PATH, 'testrepo', 'keys'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[404])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf-8'), 'Requested path data does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_check_other_exist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'HEAD',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'text/plain'),
                ('Content-Length', str(len('Test contents...'.encode('utf-8'))))
            ])
            self.assertEqual(result.decode('utf-8'), '')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_data_data_nonexist(self):
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(os.path.join(app.ROOT_PATH, 'testrepo', 'data'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[404])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf-8'), 'Requested path data does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_data_data_exist(self):
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'binary/octet-stream'),
                ('Content-Length', str(len('Test contents...'.encode('utf-8'))))
            ])
            self.assertEqual(result.decode('utf-8'), 'Test contents...')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_data_other_nonexist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(os.path.join(app.ROOT_PATH, 'testrepo', 'keys'))
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[404])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf-8'), 'Requested path data does not exist.')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_data_other_exist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'GET',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [
                ('Content-Type', 'binary/octet-stream'),
                ('Content-Length', str(len('Test contents...'.encode('utf-8'))))
            ])
            self.assertEqual(result.decode('utf-8'), 'Test contents...')
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_get_path_data_range(self):
        contents = 'Very long contents for some useful testing with HTTP Range headers. We need this to be non-trivial amounts of data so that we can get a good test.'
        tests = [
            ('bytes=15-', contents[15:]),
            ('bytes=15-25', contents[15:26]),
            ('bytes=5-10, 15-20', contents[5:11]),
            #('bytes=-15', contents[:16]), # cannot currently handle this.
        ]
        for range_header, expected_result in tests:
            env = {
                'PATH_INFO': '/testrepo/keys/aa000000000',
                'QUERY_STRING': '',
                'REQUEST_METHOD': 'GET',
                'HTTP_RANGE': range_header,
                'wsgi.errors': io.StringIO(),
                'wsgi.input': io.BytesIO(),
            }
            response = Response()
            app = self.create_application(env, response)
            self.ensure_file(
                os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000'),
                contents.encode('utf-8')
            )
            result = b''.join([x for x in app])

            try:
                self.assertEqual(response.status, resticserver.HTTP_RESPONSES[206])
                self.assertEqual(response.headers, [
                    ('Content-Type', 'binary/octet-stream'),
                    ('Content-Length', str(len(expected_result.encode('utf-8')))),
                    ('Content-Range', range_header.split(',')[0]),
                ])
                self.assertEqual(result.decode('utf-8'), expected_result)
            except:
                print(env['wsgi.errors'].getvalue())
                raise
            finally:
                shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_set_path_data_DATA(self):
        contents = 'Very long contents for some useful testing with HTTP Range headers. We need this to be non-trivial amounts of data so that we can get a good test.'
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'POST',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(contents.encode('utf-8')),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data'),
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            with open(os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa000000000'), 'rb') as infile:
                self.assertEqual(infile.read(), contents.encode('utf-8'))
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_set_path_data_other(self):
        contents = 'Very long contents for some useful testing with HTTP Range headers. We need this to be non-trivial amounts of data so that we can get a good test.'
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'POST',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(contents.encode('utf-8')),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys'),
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            with open(os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000'), 'rb') as infile:
                self.assertEqual(infile.read(), contents.encode('utf-8'))
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_delete_path_data_data_exist(self):
        env = {
            'PATH_INFO': '/testrepo/data/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'DELETE',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            self.assertFalse(os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'data', 'aa', 'aa000000000')))
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_delete_path_data_other_exist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'DELETE',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_file(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000'),
            'Test contents...'.encode('utf-8')
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[200])
            self.assertEqual(response.headers, [])
            self.assertFalse(os.path.exists(os.path.join(app.ROOT_PATH, 'testrepo', 'keys', 'aa000000000')))
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))

    def test_delete_path_data_other_notexist(self):
        env = {
            'PATH_INFO': '/testrepo/keys/aa000000000',
            'QUERY_STRING': '',
            'REQUEST_METHOD': 'DELETE',
            'wsgi.errors': io.StringIO(),
            'wsgi.input': io.BytesIO(),
        }
        response = Response()
        app = self.create_application(env, response)
        self.ensure_directory(
            os.path.join(app.ROOT_PATH, 'testrepo', 'keys'),
        )
        result = b''.join([x for x in app])

        try:
            self.assertEqual(response.status, resticserver.HTTP_RESPONSES[500])
            self.assertEqual(response.headers, [('Content-Type', 'text/plain')])
            self.assertEqual(result.decode('utf-8'), "Path data does not exist.")
        except:
            print(env['wsgi.errors'].getvalue())
            raise
        finally:
            shutil.rmtree(os.path.join(app.ROOT_PATH, 'testrepo'))


if __name__ == '__main__':
    unittest.main()
