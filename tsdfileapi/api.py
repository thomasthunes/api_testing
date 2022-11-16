
"""

tsd-file-api
------------

A multi-tenent API for uploading and downloading files and JSON data,
designed for the University of Oslo's Services for Sensitive Data (TSD).

"""

import base64
import datetime
import hashlib
import json
import logging
import os
import pwd
import queue
import re
import subprocess
import stat
import shutil
import sqlite3
import time
import traceback

from collections import OrderedDict
from http import HTTPStatus
from sys import argv
from typing import Union, Optional, Awaitable, Callable, Any
from uuid import uuid4

import libnacl.sealed
import libnacl.public
import psycopg2.errors
import magic
import tornado.httputil
import tornado.log
import yaml

from pysquril.backends import PostgresBackend, SqliteBackend
from termcolor import colored
from tornado.escape import json_decode, url_unescape, url_escape
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.options import parse_command_line, define, options
from tornado.web import (
    Application,
    RequestHandler,
    stream_request_body,
    HTTPError,
    MissingArgumentError,
)

from auth import process_access_token
from db import (
    sqlite_init,
    postgres_init,
    get_projects_migration_status,
    pg_listen_channel,
)
from exc import (
    ApiError,
    ClientError,
    ClientAuthorizationError,
    ClientReservedResourceError,
    ClientGroupAccessError,
    ClientNaclChunkSizeError,
    ClientMethodNotAllowed,
    ClientResourceNotFoundError,
    ClientContentRangeError,
    ServerStorageTemporarilyUnavailableError,
    ServerMaintenanceError,
    error_for_exception,
    Error,
)
from resumables import SerialResumable, ResumableNotFoundError, ResumableIncorrectChunkOrderError
from rmq import PikaClient
from tokens import tkn, gen_test_jwt_secrets
from utils import (
    call_request_hook,
    sns_dir,
    check_filename,
    VALID_UUID,
    md5sum,
    tenant_from_url,
    move_data_to_folder,
    set_mtime,
    choose_storage,
    find_tenant_storage_path,
    _rwxrws___,
)


_RW______ = stat.S_IREAD | stat.S_IWRITE
_RW_RW___ = _RW______ | stat.S_IRGRP | stat.S_IWGRP
_50MB = 52428800


def read_config(filename: str) -> dict:
    with open(filename) as f:
        conf = yaml.load(f, Loader=yaml.Loader)
    return conf


def set_config() -> None:
    try:
        _config = read_config(argv[1])
    except IndexError as e:
        print(colored('Missing config file, running with default setup', 'yellow'))
        print(colored('WARNING: do _not_ do this in production', 'red'))
        from defaults import _config
        from tokens import tkn
        for k, v in _config.items():
            print(colored(f'{k}:', 'yellow'), colored(f'{v}', 'magenta'))
        print(colored('JWT token for dev purposes:', 'cyan'))
        print(tkn(
            _config['jwt_test_secret'],
            exp=3600,
            role='admin_user',
            tenant='p11',
            user='p11-test')
        )
    try:
        if argv[2].startswith('--port:'):
            port = argv[2].split(':')[1]
            define('port', int(port))
        else:
            define('port', _config['port'])
    except Exception:
        define('port', _config['port'])
    define('config', _config)
    define('debug', _config['debug'])
    define('api_user', _config['api_user'])
    define('check_tenant', _config['token_check_tenant'])
    define('check_exp', _config['token_check_exp'])
    define('start_chars', _config['disallowed_start_chars'])
    define('requestor_claim_name', _config['requestor_claim_name'])
    define('tenant_claim_name', _config['tenant_claim_name'])
    define('tenant_string_pattern', _config['tenant_string_pattern'])
    define('export_chunk_size', _config['export_chunk_size'])
    define('valid_tenant', re.compile(r'{}'.format(_config['valid_tenant_regex'])))
    define('max_body_size', _config['max_body_size'])
    define('default_file_owner', _config['default_file_owner'])
    define('create_tenant_dir', _config['create_tenant_dir'])
    define('jwt_secret', _config['jwt_secret'] if 'jwt_secret' in _config.keys() else None)
    define('max_nacl_chunksize', _config.get('max_nacl_chunksize', _50MB))
    define('sealed_box', libnacl.sealed.SealedBox(
            libnacl.public.SecretKey(
                base64.b64decode(_config['nacl_public']['private'])
            )
        )
    )
    define('rabbitmq', _config.get('rabbitmq', {}))
    define('rabbitmq_cache', queue.Queue())
    define('maintenance_mode_enabled', False)
    define('request_log', _config.get('request_log'))
    if 'backlog' in _config:
        tornado.netutil.DEFAULT_BACKLOG = _config['backlog']
    if 'accept_calls_per_event_loop' in _config:
        tornado.netutil.ACCEPT_CALLS_PER_EVENT_LOOP = _config['accept_calls_per_event_loop']
    options.logging = _config.get('log_level', 'info')
    try: # limit size to 1
        projects_pool = postgres_init(
            {
                'user': _config.get('iamdb_user'),
                'pw': _config.get('iamdb_pw'),
                'host': _config.get('iamdb_host'),
                'dbname': _config.get('iamdb_dbname'),
            }
        )
    except Exception as e:
        projects_pool = None
    define('projects_pool', projects_pool)
    define('migration_statuses', get_projects_migration_status(projects_pool))
    define('tenant_storage_cache', {})
    define('prefer_ess', _config.get('prefer_ess', []))
    define('sns_migrations', _config.get('sns_migrations', []))


set_config()


def handle_iam_projects_events(
    conn: psycopg2.extensions.connection,
    events: int,
) -> bool:
    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
    logging.info(f'reloading project info')
    options.migration_statuses = get_projects_migration_status(options.projects_pool)


class AuthRequestHandler(RequestHandler):


    """
    All RequestHandler(s), with the exception of the HealthCheckHandler
    inherit from this one, giving them access to the
    process_token_and_extract_claims method.

    The purpose of this method is to allow a measure of authentication
    and authorization - just enough to be able to enforce access control
    on a per request basis, where necessary.

    When called, the method will set the following properties:

        self.jwt
        self.tenant
        self.claims
        self.requestor

    Subsequent request handlers (HTTP method implementations), use these properties
    for request processing, and enforcement of access control where needed.
    The API can be configured to check whether the tenant identifier in the URL
    matches the tenant identifier in the claims. It can also be configured
    to check the token expiry. These are not mandatory.

    To decide what is right for your use case read the module level docstring
    about the different endpoints, and use cases underpinning their design
    and implementation.

    """

    def process_token_and_extract_claims(
        self,
        check_tenant: bool = options.check_tenant,
        check_exp: bool = options.check_exp,
        tenant_claim_name: str = options.tenant_claim_name,
        verify_with_secret: bool = options.jwt_secret,
    ) -> dict:
        """
        When performing requests against the API, JWT access tokens are presented
        in the Authorization header of the HTTP request as a Bearer token. Before
        the body of each request is processed this method is called in 'prepare'.

        The process_access_token method will:
            - extract claims from the JWT
            - optionally check:
                - consistent tenant access
                - token expiry
                - signature, if provided with a secret

        The latter checks are OPTIONAL since they SHOULD already have been
        performed by an authorization server _before_ the request is handled here.

        Returns
        -------
        bool or dict

        """
        self.status = None
        try:
            auth_header = self.request.headers.get('Authorization')
            if not auth_header:
                raise ClientAuthorizationError('Authorization not possible: missing header')
            self.jwt = auth_header.split(' ')[1]

            tenant = tenant_from_url(self.request.uri)
            if not options.valid_tenant.match(tenant):
                raise ClientError(f"invalid tenant: {tenant}")
            self.tenant = tenant

            authnz = process_access_token(
                auth_header,
                tenant,
                check_tenant,
                check_exp,
                tenant_claim_name,
                verify_with_secret
            )
            if not authnz.get('status'):
                raise ClientAuthorizationError('JWT verification failed')
            self.claims = authnz.get('claims')
            self.requestor = self.claims[options.requestor_claim_name]
            return authnz
        except IndexError as e:
            raise ClientAuthorizationError('Authorization not possible: malformed header')
        except Exception as e:
            logging.error(e)
            raise ClientAuthorizationError('Authorization failed')


    def get_group_info(self, tenant: str, group_config: dict, authnz_status: dict) -> tuple:
        """
        Set intended group owner of resource, and extract memberships
        of the requestor, falling back to configured defaults, depending
        on the backend.

        """
        if not group_config['enabled']:
            return group_config['default_url_group'], group_config['default_memberships']
        try:
            group_name = url_unescape(self.get_query_argument('group'))
        except HTTPError as e:
            # first check if it is in the url
            found = re.sub(r'/v1/.+/(p[0-9]+-[a-zA-Z0-9-]+-group).*', r'\1', self.request.uri)
            if found == self.request.uri:
                # then it is not there, so we need to use the default
                default_url_group = group_config['default_url_group']
                if options.tenant_string_pattern in default_url_group:
                    group_name = default_url_group.replace(options.tenant_string_pattern, tenant)
            else:
                group_name = found
        try:
            group_memberships = authnz_status['claims']['groups']
        except Exception as e:
            logging.info('Could not get group memberships - choosing default memberships')
            default_membership = group_config['default_memberships']
            group_memberships = []
            for group in default_membership:
                if options.tenant_string_pattern in group:
                    new = group.replace(options.tenant_string_pattern, tenant)
                else:
                    new = group
                group_memberships.append(new)
        return group_name, group_memberships

    def enforce_group_logic(
        self,
        group_name: str,
        group_memberships: list,
        tenant: str,
        group_config: dict,
    ) -> None:
        """
        Optionally check that:
            - provided group name matches group name regex pattern
            - tenant name contained in provided group name
            - requestor is member of provided group name

        """
        if not group_config['enabled']:
            return
        if group_config['valid_group_regex']:
            is_valid_groupname = re.compile(r'{}'.format(group_config['valid_group_regex']))
            if not is_valid_groupname.match(group_name):
                raise ClientGroupAccessError(f"Invalid group name: {group_name}")
        if group_config['ensure_tenant_in_group_name']:
            if tenant not in group_name:
                raise ClientGroupAccessError(f"tenant: {tenant} not in group name: {group_name}")
        if group_config['enforce_membership']:
            if group_name not in group_memberships:
                raise ClientGroupAccessError(f"group: {group_name} not in memberships: {group_memberships}")

    def is_reserved_resource(self, work_dir: str, resource: str) -> bool:
        """
        Prevent access to API-owned resources.

        Criteria
        --------
        One of either:

        1. resumable db files
            - starting with .resumables-
        2. merged resumable files
            - endswith .uuid
        3. partial upload files
            - endswith .uuid.part
        4. resumable data folders
            - uuid4, has files inside with chunk.num

        Returns
        -------
        bool

        """
        resource_dir = resource.split('/')[0] if "/" in resource else resource
        if resource.startswith('.resumables-') and resource.endswith('.db'):
            logging.error(f'resumable dbs not accessible {resource}')
            return True
        elif re.match(r'(.+)\.([a-f\d0-9-]{32,36})$', resource):
            logging.error('merged resumable files not accessible')
            return True
        elif re.match(r'(.+).([a-f\d0-9-]{32,36}).part$', resource):
            logging.error('partial upload files not accessible')
            return True
        elif VALID_UUID.match(resource_dir):
            potential_target = os.path.normpath(f'{work_dir}/{resource_dir}')
            if os.path.lexists(potential_target) and os.path.isdir(potential_target):
                content = os.listdir(potential_target)
                for entry in content:
                    if re.match(r'(.+).chunk.[0-9]+$', entry):
                        logging.error(f'resumable directories not accessible {entry}')
                        return True
        return False

    def handle_mq_publication(self, mq_config: dict = None, data: dict = None) -> None:
        """
        Publish a message to RabbitMQ, as the result of a HTTP request.
        NB: The API assumes that a vhost has been created.

        Parameters
        ----------
        mq_config: dict
            enabled: bool, required
            exchange: str, optional
            version: str, e.g. v1, optional
            routing_key: str, period separated string, e.g. k.v1.foo, optional
        data: dict, no structure required

        Implementation
        --------------
        By default, messages are published with the following routing key:

            routing key: k.{version}.{tenant}.{endpoint}

        Exchanges are topic exchanges, and queues receive messages
        based on binding keys. These binding keys can have varying
        levels of specificity, such as:

            k.#                 - all
            k.v1.#              - all for v1
            k.v1.p11.#          - all for v1, and tenant p11
            k.v1.p11.survey     - only for v1, p11 and the survey backend

        For a backend to produce messages, it's config must be set as, e.g.:

            mq:
              enabled: True
              exchange: 'ex_apps'
              version: 'v1'
              methods:
                GET: False
                HEAD: False
                PUT: True
                PATCH: True
                DELETE: True

        Defaults can be over-ridden by providing mq_config.

        Error handling
        --------------
        If the broker is down, then the connection, and channel will be closed.
        While it is down, messages that fail to be published will be placed in
        an in-memory queue. When the broker is available again, the API will try
        to reconnect, and get a new channel. Once available, the API will publish
        all messages in the queue. Such messagess are published with the timestamp
        at which they would have been published originally.

        If either the connection or channel is closed for different reasons than
        the broker being down, the same process will be followed.

        Naturally, if the process restarts while the broker is unavailable, enqueued
        messages will be lost.

        """
        if not self.application.settings.get('pika_client'):
            logging.info('no pika_client found')
            return
        if not mq_config:
            return
        if not mq_config.get('enabled'):
            return
        if not mq_config.get('methods').get(self.request.method):
            return
        try:
            default_version = 'v1'
            default_rkey = f'k.{default_version}.{self.tenant}.{self.endpoint}'
            ex = mq_config.get('exchange')
            ver = (
                default_version if not mq_config.get('version')
                else mq_config.get('version')
            )
            rkey = (
                default_rkey if not mq_config.get('routing_key')
                else mq_config.get('routing_key')
            )
            uri = (
                self.request.headers.get('Original-Uri') if self.request.headers.get('Original-Uri')
                else self.request.uri
            )
            # try hard to be able to publish, if e.g. the broker restarted
            connection_open = self.application.settings.get('pika_client').connection.is_open
            re_open_connection = False
            if connection_open:
                channel_open = self.application.settings.get('pika_client').channel.is_open
                if not channel_open:
                    logging.info('RabbitMQ channel is closed')
                    re_open_connection = True
            if not connection_open:
                logging.info('RabbitMQ connection is closed')
                re_open_connection = True
            if re_open_connection:
                logging.info('trying to re-open RabbitMQ connection')
                self.application.settings.get('pika_client').connect()
            self.pika_client = self.application.settings.get('pika_client')
            self.pika_client.publish_message(
                exchange=ex,
                routing_key=rkey,
                method=self.request.method,
                uri=uri,
                version=ver,
                data=data,
            )
            if not options.rabbitmq_cache.empty(): # if the broker was down
                try:
                    logging.info(f'publishing {options.rabbitmq_cache.qsize()} messages from cache')
                    message = options.rabbitmq_cache.get_nowait()
                    while message:
                        self.pika_client.publish_message(
                            exchange=message.get('ex'),
                            routing_key=message.get('rkey'),
                            method=message.get('method'),
                            uri=message.get('uri'),
                            version=message.get('version'),
                            data=message.get('data'),
                            timestamp=message.get('timestamp'),
                        )
                        message = options.rabbitmq_cache.get_nowait()
                except queue.Empty:
                    pass # nothing left
        except (Exception, UnboundLocalError) as e:
            summary = f'exchange: {ex}, routing_key: {rkey}, version: {ver}'
            msg = f'problem publishing message, {summary}'
            logging.error(msg)
            logging.error(e)
            options.rabbitmq_cache.put(
                {
                    'ex': ex,
                    'rkey': rkey,
                    'method': self.request.method,
                    'uri': uri,
                    'version': ver,
                    'data': data,
                    'timestamp': int(time.time()),
                }
            )

    # audit log tools

    def _log_db_name(self, backend: str, app: str) -> str:
        name = f'apps_{app}' if app else backend
        return f'.request_log_{name}.db'

    def _log_table_name(self, backend: str, app: str) -> str:
        return f'{backend}' if not app else f'apps_{app}'

    def _log_db_path(self, backend: str, tenant: str) -> str:
        path_pattern = options.request_log.get('db').get('path')
        return choose_storage(
            tenant=tenant,
            endpoint_backend=backend,
            opts=options,
            directory=path_pattern.replace(options.tenant_string_pattern, tenant),
        )

    def _sqlite_log_engine(
        self,
        tenant: str,
        backend: str,
        app: str,
    ) -> sqlite3.Connection:
        db_path = self._log_db_path(backend, tenant)
        db_name = self._log_db_name(backend, app)
        engine = sqlite_init(db_path, name=db_name, builtin=True)
        return engine

    def get_log_db(
        self,
        tenant: str,
        backend: str,
        engine_type: str,
        app: str = None,
    ) -> Union[SqliteBackend, PostgresBackend]:
        if engine_type == 'postgres':
            db = PostgresBackend(options.pgpool_request_log, schema=tenant)
        elif engine_type == 'sqlite':
            engine = self._sqlite_log_engine(tenant, backend, app)
            db = SqliteBackend(engine)
        return db

    def get_app_name(self, uri: str) -> str:
        if '/apps/' not in uri:
            return None
        parts = uri.split('apps')
        return parts[1].split('/')[1]

    def get_log_apps(self, tenant: str, backend: str, engine_type: str) -> list:
        if engine_type == 'postgres':
            db = PostgresBackend(options.pgpool_request_log, schema=tenant)
            tables = db.tables_list()
            return [table for table in tables if table.startswith('apps_')]
        elif engine_type == 'sqlite':
            path = self._log_db_path(backend, tenant)
            dbs = os.listdir(path)
            return [db for db in dbs if db.endswith('.db')]


    def update_request_log(
        self,
        *,
        tenant: str,
        backend: str,
        requestor: str,
        method: str,
        uri: str,
        app: str = None,
        claims: dict = {},
    ) -> None:
        """
        Log request information - for audit purposes.

        """
        try:
            backends = options.request_log.get('backends')
            if (
                not options.request_log
                or not backends.get(backend)
                or method not in backends.get(backend).get('methods')
            ):
                return
        except (AttributeError, KeyError, AssertionError) as e:
            return
        try:
            engine_type = options.request_log.get('db').get('engine')
            assert engine_type in ['postgres', 'sqlite'], \
                f'unsupported engine_type: {engine_type}'
            db = self.get_log_db(tenant, backend, engine_type, app)
            data = {
                'timestamp': datetime.datetime.utcnow().isoformat(),
                'requestor': requestor,
                'method': method,
                'uri': uri,
            }
            added_claims = backends.get('claims')
            if added_claims:
                info = {}
                for claim in added_claims:
                    info[claim] = claims.get(claim)
                data['claims'] = info
            table_name = self._log_table_name(backend, app)
            db.table_insert(table_name, data)
        except Exception as e:
            logging.warning(f'could not update audit log: {e}')


class SnsFormDataHandler(AuthRequestHandler):

    """
    Implements processing files uploaded via multipart/form-data,
    for legacy Nettskjema.

    """

    def initialize(self, backend: str) -> None:
        self.backend = backend
        self.endpoint = self.request.uri.split('/')[3]
        self.tenant = tenant_from_url(self.request.uri)
        current_backend = options.config['backends']['disk'][backend]
        self.tenant_dir_pattern = current_backend.get('import_path')
        self.tsd_hidden_folder_pattern = current_backend.get('subfolder_path')
        self.request_hook = current_backend.get('request_hook')
        self.check_tenant = current_backend.get('check_tenant')
        self.group_config = current_backend.get('group_logic', {
                'enabled': False,
                'default_url_group': '',
                'default_memberships': [],
                'ensure_tenant_in_group_name': False,
                'valid_group_regex': None,
                'enforce_membership': False,
            }
        )

    def prepare(self) -> Optional[Awaitable[None]]:
        try:
            if not options.valid_tenant.match(self.tenant):
                raise ClientError(f"malformed tenant: {self.tenant}")
            if options.maintenance_mode_enabled:
                raise ServerMaintenanceError
            self.new_paths = []
            self.group_name = None
            self.authnz = self.process_token_and_extract_claims(
                check_tenant=self.check_tenant if self.check_tenant is not None else options.check_tenant
            )
            if not self.request.files['file']:
                raise ClientError("No files included in the request")
            group_name, group_memberships = self.get_group_info(
                self.tenant, self.group_config, self.authnz,
            )
            self.enforce_group_logic(
                group_name, group_memberships, self.tenant, self.group_config,
            )
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()

    def write_files(self, filemode: str, tenant: str) -> None:
        for current_file in self.request.files['file']:
            filename = check_filename(
                current_file['filename'],
                disallowed_start_chars=options.start_chars,
            )
            filebody = current_file['body']
            if len(filebody) == 0:
                raise ClientError('empty file body')
            self.write_file(filemode, filename, filebody, tenant)

    def write_file(self, filemode: str, filename: str, filebody: bytes, tenant: str) -> None:

        # find storage backend preferences
        _  = find_tenant_storage_path(self.tenant, self.backend, options) # update cache
        tenant_sns_info = options.tenant_storage_cache.get(tenant, {}).get("sns", {})
        use_hnas = tenant_sns_info.get("hnas", True)
        use_ess = (
            tenant_sns_info.get("ess")
            and (tenant in options.sns_migrations or "all" in options.sns_migrations)
        )
        copy_to_ess = use_hnas and use_ess

        # create form directories where needed
        tsd_hidden_folder = sns_dir(
            self.tsd_hidden_folder_pattern,
            tenant,
            self.request.uri,
            options.tenant_string_pattern,
            options=options,
            use_hnas=use_hnas,
            use_ess=use_ess,
        )
        tenant_dir = sns_dir(
            self.tenant_dir_pattern,
            tenant,
            self.request.uri,
            options.tenant_string_pattern,
            options=options,
            use_hnas=use_hnas,
            use_ess=use_ess,
        )

        # ensure idempotent uploads
        self.path = os.path.normpath(f"{tenant_dir}/{filename}")
        self.path_part = f"{self.path}.{str(uuid4())}.part"
        if os.path.lexists(self.path):
            logging.debug(f'{self.path} exists, renaming to {self.path_part}')
            os.rename(self.path, self.path_part)

        # write to partial file, rename, set permissions
        with open(self.path_part, filemode) as f:
            f.write(filebody)
            os.rename(self.path_part, self.path)
            os.chmod(self.path, _RW_RW___)

        # now copy the origin file to the folder intended to data processing
        subfolder_path = os.path.normpath(tsd_hidden_folder + '/' + filename)
        shutil.copy(self.path, subfolder_path)
        os.chmod(subfolder_path, _RW_RW___)

        if copy_to_ess:
            try:
                target = ""
                ess_path = options.tenant_storage_cache.get(tenant, {}).get("storage_paths", {}).get("ess")
                hnas_durable = f"/tsd/{self.tenant}/data/durable"

                # 1. project-visible file
                target = self.path.replace(hnas_durable, ess_path)
                logging.info(f"copying {target} to ess")
                shutil.copy(self.path, target)
                os.chmod(target, _RW_RW___)

                # 2. data processing file
                target = subfolder_path.replace(hnas_durable, ess_path)
                logging.info(f"copying {target} to ess")
                shutil.copy(self.path, target)
                os.chmod(target, _RW_RW___)
            except Exception as e:
                logging.error(e)
                raise ServerSnsError(f"could not copy {target} to ESS")

    def on_finish(self) -> None:
        if (
            self.request.method in ('PUT', 'POST', 'PATCH')
            and self.request_hook.get('enabled')
        ):
            for path in self.new_paths:
                call_request_hook(
                    self.request_hook['path'],
                    [path, self.requestor, options.api_user, self.group_name],
                    as_sudo=self.request_hook['sudo'],
                )

    def handle_data(self, filemode: str, tenant: str) -> None:
        try:
            self.write_files(filemode, tenant)
            self.set_status(HTTPStatus.CREATED.value)
            self.write({'message': 'data uploaded'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': 'could not upload data'})

    def put(self, tenant: str, keyid: str, formid: str) -> None:
        self.handle_data('wb+', tenant)


class ResumablesHandler(AuthRequestHandler):

    """
    Manage resumables, report information.

    Implementation
    --------------
    To continue a resumable upload which has stopped, clients
    can get information about data stored by the server.

    This class provides a GET method, which returns the relevant
    information to the client, for a given upload_id, and/or file.

    This information is:

    - filename
    - sequence number of last chunk
    - chunk size
    - upload id
    - md5sum of the last chunk
    - previos offset in bytes
    - next offset in bytes (total size of merged file)
    - resumable key (server-side directory specified in url)

    There are two possible scenarios: 1) the client knows the upload_id
    associated with the file which needs to be resumed, or 2) the client
    only knows the name of the file which needs to be resumed.

    Scenario 1
    ----------
    In scenario #1, the upload_id is provided, and the server returns
    the information.

    Scenario 2
    ----------
    In scenario #2, the server will look at all potential resumables,
    and try to find a match based on the filename. All relevant matches
    to which the authenticated user has access are returned, including
    information about how much data has been uploaded. The client can
    then choose to resume the one with the most data, and delete the
    remaining ones.

    """

    def initialize(self, backend: str) -> None:
        self.backend = backend
        self.endpoint = self.request.uri.split('/')[3]
        self.tenant = tenant_from_url(self.request.uri)
        self.import_dir_pattern = options.config['backends']['disk'][backend]['import_path']
        self.check_tenant = options.config['backends']['disk'][backend].get('check_tenant')

    def prepare(self) -> Optional[Awaitable[None]]:
        try:
            self.tenant_dir = choose_storage(
                tenant=self.tenant,
                endpoint_backend=self.backend,
                opts=options,
                directory=self.import_dir_pattern.replace(options.tenant_string_pattern, self.tenant),
            )
            if options.maintenance_mode_enabled:
                raise ServerMaintenanceError
            self.authnz = self.process_token_and_extract_claims(
                check_tenant=self.check_tenant if self.check_tenant is not None else options.check_tenant
            )
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()

    def get(self, tenant: str, filename: str = None) -> None:
        try:
            upload_id = url_unescape(self.get_query_argument('id', ''))
            key = url_unescape(self.get_query_argument('key', ''))
            res = SerialResumable(self.tenant_dir, self.requestor)
            if not filename:
                info = res.list_all(self.tenant_dir, self.requestor, key=key)
            else:
                info = res.info(
                    self.tenant_dir, filename, upload_id, self.requestor, key=key,
                )
            self.set_status(HTTPStatus.OK.value)
            self.write(info)
        except ResumableNotFoundError as e:
            self.set_status(HTTPStatus.NOT_FOUND.value)
            self.write({"message": "not found"})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write(
                {
                    'filename': filename,
                    'id': None,
                    'chunk_size': None,
                    'max_chunk': None,
                    'md5sum': None,
                    'previous_offset': None,
                    'next_offset': None,
                    'warning': None,
                    'group': None,
                    'key': None,
                }
            )

    def delete(self, tenant: str, filename: str) -> None:
        try:
            upload_id = url_unescape(self.get_query_argument('id'))
            res = SerialResumable(self.tenant_dir, self.requestor)
            if not res.delete(self.tenant_dir, filename, upload_id, self.requestor):
                # TODO: raise from ^
                raise ClientAuthorizationError("resumable access denied")
            self.set_status(HTTPStatus.OK.value)
            self.write({'message': 'resumable deleted'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)


@stream_request_body
class FileRequestHandler(AuthRequestHandler):

    def decrypt_nacl_headers(self, headers: tornado.httputil.HTTPHeaders) -> None:
        if not headers.get('Nacl-Chunksize'):
            raise ClientError('Missing Nacl-Chunksize header - cannot decrypt data')
        self.custom_content_type = headers['Content-Type']
        self.nacl_stream_buffer = b''
        self.nacl_nonce = options.sealed_box.decrypt(
            base64.b64decode(headers['Nacl-Nonce'])
        )
        self.nacl_key = options.sealed_box.decrypt(
            base64.b64decode(headers['Nacl-Key'])
        )
        self.nacl_chunksize = int(headers['Nacl-Chunksize'])


    def initialize(self, backend: str, namespace: str, endpoint: str) -> None:
        default_group_logic = {
            'enabled': False,
            'default_url_group': '',
            'default_memberships': [],
            'ensure_tenant_in_group_name': False,
            'valid_group_regex': None,
            'enforce_membership': False
        }
        self.backend = backend
        self.namespace = namespace
        self.endpoint = endpoint
        self.tenant = tenant_from_url(self.request.uri)
        self.allow_export = options.config['backends']['disk'][backend]['allow_export']
        self.allow_list = options.config['backends']['disk'][backend]['allow_list']
        self.allow_info = options.config['backends']['disk'][backend]['allow_info']
        self.allow_delete = options.config['backends']['disk'][backend]['allow_delete']
        self.export_max = options.config['backends']['disk'][backend]['export_max_num_list']
        self.has_posix_ownership = options.config['backends']['disk'][backend]['has_posix_ownership']
        self.check_tenant = options.config['backends']['disk'][backend].get('check_tenant')
        self.mq_config = options.config['backends']['disk'][backend].get('mq')
        self.request_hook = options.config['backends']['disk'][backend]['request_hook']
        self.use_original_uri = options.config['backends']['disk'][backend].get('use_original_uri')
        self.group_config = options.config['backends']['disk'][backend].get('group_logic', default_group_logic)
        self.CHUNK_SIZE = options.export_chunk_size
        self.backend_paths = options.config['backends']['disk'][backend]
        self.export_path_pattern = self.backend_paths['export_path']
        self.export_policy = options.config['backends']['disk'][backend]['export_policy']
        self.import_dir_pattern = options.config['backends']['disk'][backend]['import_path']


    @gen.coroutine
    def prepare(self) -> Optional[Awaitable[None]]:
        self.error = None
        self.completed_resumable_file = False
        self.target_file = None
        self.custom_content_type = None
        self.path = None
        self.path_part = None
        self.chunk_order_correct = True
        self.on_finish_called = False
        self.listing_dir = False
        self.filename = None
        self.chunk_num = None
        filemode = "wb+"
        try:
            if options.maintenance_mode_enabled:
                raise ServerMaintenanceError

            self.export_dir = choose_storage(
                tenant=self.tenant,
                endpoint_backend=self.backend,
                opts=options,
                directory=self.export_path_pattern.replace(options.tenant_string_pattern, self.tenant),
            )
            self.import_dir = choose_storage(
                tenant=self.tenant,
                endpoint_backend=self.backend,
                opts=options,
                directory=self.import_dir_pattern.replace(options.tenant_string_pattern, self.tenant),
            )
            self.tenant_dir = choose_storage(
                tenant=self.tenant,
                endpoint_backend=self.backend,
                opts=options,
                directory=self.import_dir_pattern.replace(options.tenant_string_pattern, self.tenant),
            )

            self.authnz = self.process_token_and_extract_claims(
                check_tenant=self.check_tenant if self.check_tenant is not None else options.check_tenant
            )

            tenant = tenant_from_url(self.request.uri)
            if not options.valid_tenant.match(tenant):
                raise ClientAuthorizationError('URI does not contain a valid tenant')
            self.tenant = tenant

            uri = self.request.uri.split('?')[0]
            uri_parts = uri.split('/')
            if uri.endswith("/files/stream") and self.request.method in ['PUT', 'PATCH']:
                # Some legacy clients might not include the filename in the URL
                # but instead in the Filename header :(
                logging.warning(f'legacy Filename header used: {self.request.uri}')
                filename = self.request.headers.get('Filename', f"{datetime.datetime.now().isoformat()}.txt")
                uri_parts.append(filename)
                uri = f'{uri}/{filename}'
            else:
                filename = uri_parts[-1]

            self.filename = check_filename(
                url_unescape(filename),
                disallowed_start_chars=options.start_chars,
            ) if filename else None

            # ensure resource is not reserved
            delimiter = self.endpoint or self.namespace
            resource_parts = uri.split(f'/{delimiter}/')
            resource = resource_parts[-1] if resource_parts else None
            if self.request.method in ('GET', 'HEAD', 'DELETE'):
                work_dir = self.export_dir
            elif self.request.method in ('PUT', 'PATCH'):
                work_dir = self.import_dir
            elif not resource and self.request.method == "DELETE":
                raise ClientError("no resource to delete")
            if resource == uri:
                pass # cannot be reserved, no need to check
            else:
                if resource and self.is_reserved_resource(work_dir, url_unescape(resource)):
                    raise ClientReservedResourceError(f"{resource} name not allowed")


            # Check headers
            header_keys = self.request.headers.keys()
            if 'Content-Type' not in header_keys:
                content_type = 'application/octet-stream'
                self.request.headers['Content-Type'] = 'application/octet-stream'
            elif 'Content-Type' in header_keys:
                content_type = self.request.headers['Content-Type']

            if content_type == 'application/octet-stream+nacl':
                required_nacl_headers = ['Nacl-Key', 'Nacl-Nonce', 'Nacl-Chunksize']
                for required_nacl_header in required_nacl_headers:
                    if required_nacl_header not in header_keys:
                        raise ClientError(f'missing {required_nacl_header}')
                if int(self.request.headers['Nacl-Chunksize']) > options.max_nacl_chunksize:
                    raise ClientNaclChunkSizeError(
                        f'Nacl-Chunksize larger than max allowed: {options.max_nacl_chunksize}'
                    )

            # Validate groups, and group logic
            group_name, group_memberships = self.get_group_info(tenant, self.group_config, self.authnz)
            self.enforce_group_logic(group_name, group_memberships, tenant, self.group_config)
            self.group_name = group_name

            if self.group_config['enabled'] and self.request.method in ['PUT', 'PATCH']:
                # if a directory is present, and not the same as the group
                uri_group_name = uri_parts[5]
                if url_unescape(uri_group_name) != self.filename and uri_group_name != group_name:
                    raise ClientGroupAccessError(
                        f'inconsistent group permissions {group_name} != {uri_group_name}'
                    )
                # if group folder not in url, inject it from group_name
                if url_unescape(uri_group_name) == self.filename:
                    # inject appropriate value - until clients have transitioned
                    file = url_escape(self.filename)
                    resource = f'{group_name}/{file}'
            elif self.group_config['enabled'] and self.request.method in ['GET', 'HEAD']:
                # then we are operating in the TSD import directory
                if not self.filename and len(uri_parts) == 5: # no folder given
                    resource = work_dir # root dir

            self.resource = resource
            if self.request.method in ('PUT', 'PATCH'):

                filename = self.filename
                # optionally create dirs
                if options.create_tenant_dir:
                    if not os.path.lexists(self.tenant_dir):
                        os.makedirs(self.tenant_dir)

                url_dirs = os.path.dirname(resource)
                self.resource_dir = os.path.normpath(f'{self.tenant_dir}/{url_dirs}')
                if not os.path.lexists(self.resource_dir):
                    logging.info(f'creating resource dir: {self.resource_dir}')
                    os.makedirs(self.resource_dir)
                    target = self.tenant_dir
                    # optionally set permissions
                    if self.group_config['enabled']:
                        for _dir in url_dirs.split('/'):
                            target += f'/{_dir}'
                            os.chmod(target, _rwxrws___())
                            owner = options.api_user  # so it can move the file into the dir
                            subprocess.call(['sudo', 'chown', f'{owner}:{self.group_name}', target])

                # handle resumable
                if self.request.method == 'PATCH':
                    # select resumable key:
                    # then we remove the group name from url_dirs
                    # because this is handled transparently
                    # (for better or for worse)
                    if self.group_config['enabled']:
                        self.res_key = url_dirs.replace(self.group_name, '')[1:] # strip starting /
                        self.res_key = None if not self.res_key else self.res_key
                    else:
                        self.res_key = url_dirs
                    self.res = SerialResumable(self.tenant_dir, self.requestor)
                    url_chunk_num = url_unescape(self.get_query_argument('chunk'))
                    url_upload_id = url_unescape(self.get_query_argument('id', ''))
                    self.chunk_num, \
                    self.upload_id, \
                    self.completed_resumable_file, \
                    self.chunk_order_correct, \
                    filename = self.res.prepare(
                            self.tenant_dir,
                            filename,
                            url_chunk_num,
                            url_upload_id,
                            self.group_name,
                            self.requestor,
                            self.res_key,
                        )
                    if not self.chunk_order_correct:
                        # TODO: raise from prepare ^
                        raise ResumableIncorrectChunkOrderError

                self.path = os.path.normpath(f"{self.tenant_dir}/{filename}")
                self.path_part = f"{self.path}.{str(uuid4())}.part"

                # ensure idempotency
                if os.path.lexists(self.path):
                    if os.path.isdir(self.path):
                        logging.info('directory: %s already exists due to prior upload, removing', self.path)
                        shutil.rmtree(self.path)
                    else:
                        logging.info('%s already exists, renaming to %s', self.path, self.path_part)
                        os.rename(self.path, self.path_part)

                self.path, self.path_part = self.path_part, self.path

                if content_type == 'application/octet-stream+nacl':
                    self.decrypt_nacl_headers(self.request.headers)
                    self.target_file = open(self.path, filemode)
                    os.chmod(self.path, _RW______)
                else:
                    if self.request.method != 'PATCH':
                        self.target_file = open(self.path, filemode)
                        os.chmod(self.path, _RW______)
                    elif self.request.method == 'PATCH':
                        if not self.completed_resumable_file:
                            self.target_file = self.res.open_file(self.path, filemode)
        except ResumableIncorrectChunkOrderError as e:
            self.set_status(HTTPStatus.BAD_REQUEST.value)
            self.finish({'message': 'chunk_order_incorrect'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()

    @gen.coroutine
    def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        try:
            if not self.custom_content_type:
                if self.request.method == 'PATCH':
                    self.res.add_chunk(self.target_file, chunk)
                else:
                    self.target_file.write(chunk)
            elif self.custom_content_type == 'application/octet-stream+nacl':
                self.nacl_stream_buffer += chunk
                while len(self.nacl_stream_buffer) >= self.nacl_chunksize:
                    target_content = self.nacl_stream_buffer[:self.nacl_chunksize]
                    remainder = self.nacl_stream_buffer[self.nacl_chunksize:]
                    self.nacl_stream_buffer = remainder
                    decrypted = libnacl.crypto_stream_xor(
                        target_content,
                        self.nacl_nonce,
                        self.nacl_key
                    )
                    if self.request.method == 'PATCH':
                        self.res.add_chunk(self.target_file, decrypted)
                    else:
                        self.target_file.write(decrypted)
        except Exception as e:
            logging.error(e)
            logging.error("something went wrong with stream processing have to close file")
            if self.target_file:
                self.target_file.close()
            os.rename(self.path, self.path_part)
            self.send_error("something went wrong")

    def put(self, tenant: str, uri_filename: str = None) -> None:
        if not self.custom_content_type:
            self.target_file.close()
            os.rename(self.path, self.path_part)
        elif self.custom_content_type == 'application/octet-stream+nacl':
            if self.nacl_stream_buffer:
                decrypted = libnacl.crypto_stream_xor(
                    self.nacl_stream_buffer,
                    self.nacl_nonce,
                    self.nacl_key
                )
                self.nacl_stream_buffer = b''
                self.target_file.write(decrypted)
            self.target_file.close()
            os.rename(self.path, self.path_part)
        self.set_status(HTTPStatus.CREATED.value)
        self.write({'message': 'data streamed'})


    def patch(self, tenant: str, uri_filename: str = None) -> None:
        if (
            self.custom_content_type == 'application/octet-stream+nacl'
            and self.nacl_stream_buffer
        ):
            decrypted = libnacl.crypto_stream_xor(
                self.nacl_stream_buffer,
                self.nacl_nonce,
                self.nacl_key
            )
            self.nacl_stream_buffer = b''
            self.res.add_chunk(self.target_file, decrypted)
        if not self.completed_resumable_file:
            self.res.close_file(self.target_file)
            # if the path to which we want to rename the file exists
            # then we have been writing the same chunk concurrently
            # from two different processes, so we should not do it
            if not os.path.lexists(self.path_part):
                os.rename(self.path, self.path_part)
                filename = os.path.basename(self.path_part).split('.chunk')[0]
                try:
                    self.res.merge_chunk(
                        self.tenant_dir,
                        os.path.basename(self.path_part),
                        self.upload_id,
                        self.requestor
                    )
                    self.set_status(HTTPStatus.CREATED.value)
                    self.write(
                        {
                            'filename': filename,
                            'id': self.upload_id,
                            'max_chunk': self.chunk_num,
                            'key': self.res_key,
                        }
                    )
                except Exception as e:
                    logging.error(e)
                    error = error_for_exception(e)
                    self.set_status(error.status, reason=error.reason)
                    self.write(
                        {
                            'filename': filename,
                            'id': self.upload_id,
                            'max_chunk': self.chunk_num - 1, # we failed
                            'key': self.res_key,
                        }
                    )
            else:
                self.set_status(HTTPStatus.BAD_REQUEST.value)
                self.write({'message': 'chunk_order_incorrect'})
                return
        else:
            try:
                self.completed_resumable_filename = self.res.finalise(
                    self.tenant_dir,
                    os.path.basename(self.path_part),
                    self.upload_id,
                    self.requestor
                )
            except ResumableNotFoundError:
                self.set_status(HTTPStatus.BAD_REQUEST.value)
                return
            filename = os.path.basename(self.completed_resumable_filename)
            self.set_status(HTTPStatus.CREATED.value)
            self.write(
                {
                    'filename': filename,
                    'id': self.upload_id,
                    'max_chunk': self.chunk_num,
                    'key': self.res_key,
                }
            )

    def enforce_export_policy(
        self,
        policy_config: dict,
        filename: str,
        tenant: str,
        size: int,
        mime_type: str,
    ) -> bool:
        """
        Check file to ensure it meets the requirements of the export policy

        Checks
        ------
        1. For all tenants, check that the file name follows conventions
        2. For the given tenant, if a policy is specified and enabled check:
            - file size does not exceed max allowed for export
            - MIME types conform to allowed types, if policy enabled

        Returns
        -------
        (bool, <str,None>, <int,None>),
        (is_conformant, mime-type, size)

        """
        status = False # until proven otherwise
        try:
            file = os.path.basename(filename)
            check_filename(file, disallowed_start_chars=options.start_chars)
        except Exception as e:
            logging.error(f"Illegal export filename: {file}")
            return status
        if tenant in policy_config.keys():
            policy = policy_config[tenant]
        else:
            policy = policy_config['default']
        if not policy['enabled']:
            status = True
            return status
        if '*' in policy['allowed_mime_types']:
            status = True
        else:
            status = True if mime_type in policy['allowed_mime_types'] else False
            if not status:
                logging.error(f'not allowed to export file with MIME type: {mime_type}')
        if policy['max_size'] and size > policy['max_size']:
            logging.error(f'{self.requestor} tried to export a file exceeding the maximum size limit')
            status = False
        return status


    def get_file_metadata(self, filename: str) -> tuple:
        filename_raw_utf8 = filename.encode('utf-8')
        mime_type = 'unknown'
        try:
            mime_type = magic.from_file(filename_raw_utf8, mime=True)
        except IsADirectoryError:
            mime_type = 'directory'
        except PermissionError:
            # so the API user can read, and delete files owned by others
            if os.path.isdir(filename):
                subprocess.call(['sudo', 'chmod', '-R', 'g+r,o+rx', filename])
                mime_type = 'directory'
            else:
                subprocess.call(['sudo', 'chmod', 'g+r,o+rx', filename])
                mime_type = magic.from_file(filename_raw_utf8, mime=True)
        size = os.stat(filename).st_size
        mtime = os.stat(filename).st_mtime
        return size, mime_type, mtime


    def _base_uri(self) -> str:
        """
        When running the API behind a proxy, it may be necessary
        to use the proxy-level URI to generate correct href values
        in the metadata produced by self.list_files

        This method determines which URI to use based on backend
        specific configuration as such:

        {backend}:
          use_original_uri:
            header_value: str
            claim_conditions:
              any: bool
              claim_key: str
              claim_value: str

        Where claim_key and claim_value refer to claims in the
        requestors' JWT.

        """
        if self.use_original_uri:
            header_value = self.use_original_uri.get('header_value')
            conditions = self.use_original_uri.get('claim_conditions')
            if conditions.get('any'):
                uri = self.request.headers.get(header_value)
            else:
                key = self.claims.get(conditions.get('claim_key'))
                val = self.claims.get(conditions.get('claim_value'))
                if self.claims.get(key) == val:
                    uri = self.request.headers.get(header_value)
                else:
                    uri = self.request.uri
        else:
            uri = self.request.uri
        if not uri:
            uri = self.request.uri
        return uri.split('?')[0]


    def list_files(self, path: str, tenant: str, root: str) -> None:
        """
        List a directory.

        Listing the export directory requires changing folder
        and file modes in order to stat inodes.

        Listing the import directory has its own special semantics.
        The requestor is only allowed to list those group folders
        which intersect with their group memberships. At the top
        level folder, only folders are allowed.

        When the backend does not have has_posix_ownership and/or
        group logic, the API owns everything, and listing is simple.

        Returns
        -------
        dict

        """
        self.listing_dir = True
        current_page = 0
        pagination_value = 100
        disable_metadata = self.get_query_argument('disable_metadata', None)
        try:
            current_page = int(self.get_query_argument('page'))
            pagination_value = int(self.get_query_argument('per_page'))
        except HTTPError as e:
            pass # use default value
        except ValueError:
            raise ClientError('next values must be integers')
        if current_page < 0:
            raise ClientError('next values are natural numbers')
        if pagination_value > 50000:
            raise ClientError('per_page cannot exceed 1000')
        # arbitrary order
        # if not returning what you want
        # then try next page
        dir_map = os.scandir(path)
        paginate = False
        files = []
        start_at = (current_page * pagination_value) - 1
        stop_at = start_at + pagination_value
        # only materialise the necessary entries
        for num, entry in enumerate(dir_map):
            if num <= start_at:
                continue
            elif num <= stop_at and num >= start_at:
                files.append(entry)
            elif num == stop_at + 1:
                paginate = True
                break # there is more
        if len(files) == 0:
            self.write({'files': [], 'page': None})
        else:
            if paginate and not current_page:
                next_page = 1
            elif paginate:
                next_page = int(current_page) + 1
            else:
                next_page = None
            baseuri = self._base_uri()
            nextref = f'{baseuri}?page={next_page}' if next_page else None
            if self.export_max and len(files) > self.export_max:
                raise ClientError(f'number of files exceed configured maximum: {self.export_max}')
            names = []
            times = []
            exportable = []
            reasons = []
            sizes = []
            mimes = []
            owners = []
            etags = []
            mtimes = []
            if not self.group_config['enabled']:
                default_owner = options.default_file_owner.replace(options.tenant_string_pattern, tenant)
                for file in files:
                    filepath = file.path
                    size, mime_type, latest = self.get_file_metadata(filepath)
                    status = self.enforce_export_policy(self.export_policy, filepath, tenant, size, mime_type)
                    reason = None if status else "not allowed"
                    path_stat = file.stat()
                    etag = self.mtime_to_digest(latest)
                    date_time = str(datetime.datetime.fromtimestamp(latest).isoformat())
                    if self.has_posix_ownership:
                        try:
                            owner = pwd.getpwuid(path_stat.st_uid).pw_name
                        except KeyError:
                            try:
                                default_owner_id = pwd.getpwnam(default_owner).pw_uid
                                group_id = path_stat.st_gid
                                os.chown(file.path, default_owner_id, group_id)
                                owner = default_owner
                            except Exception:
                                logging.error(f'could not reset owner of {filepath} to {default_owner}')
                                owner = 'nobody'
                    else:
                        owner = options.api_user
                    names.append(os.path.basename(filepath))
                    times.append(date_time)
                    exportable.append(status)
                    reasons.append(reason)
                    sizes.append(size)
                    mimes.append(mime_type)
                    owners.append(owner)
                    etags.append(etag)
                    mtimes.append(latest)
            else: # then it is the TSD import dir
                group_memberships = self.claims.get('groups')
                if root:
                    for file in files:
                        if file.name in group_memberships:
                            names.append(os.path.basename(file.path))
                            times.append(None)
                            exportable.append(False)
                            reasons.append(None)
                            sizes.append(None)
                            mimes.append(None)
                            owners.append(None)
                else:
                    for file in files:
                        if not disable_metadata:
                            path_stat = file.stat()
                            latest = path_stat.st_mtime
                            etag = self.mtime_to_digest(latest)
                            date_time = str(datetime.datetime.fromtimestamp(latest).isoformat())
                            size, mime_type, mtime = self.get_file_metadata(file.path)
                        else:
                            date_time = None
                            etag = None
                            size, mime_type, mtime = None, None, None
                        names.append(file.name)
                        times.append(date_time)
                        exportable.append(False)
                        reasons.append(None)
                        sizes.append(size)
                        mimes.append(mime_type)
                        owners.append(None)
                        etags.append(etag)
                        mtimes.append(mtime)
            file_info = []
            for f, t, e, r, s, m, o, g, d in zip(
                names, times, exportable, reasons, sizes, mimes, owners, etags, mtimes
            ):
                href = f'{baseuri}/{url_escape(f)}'
                file_info.append(
                    {
                        'filename': f,
                        'size': s,
                        'modified_date': t,
                        'href': href,
                        'exportable': e,
                        'reason': r,
                        'mime-type': m,
                        'owner': o,
                        'etag': g,
                        'mtime': d
                    }
                )
            logging.info(f'{self.requestor} listed {path}')
            self.write({'files': file_info, 'page': nextref})

    def mtime_to_digest(self, mtime: int) -> str:
        return hashlib.md5(str(mtime).encode('utf-8')).hexdigest()


    def compute_etag(self) -> Optional[str]:
        """
        If there is a file resource, compute the Etag header.
        Custom values: md5sum of string value of last modified
        time of file. Client can get this value before staring
        a download, and then if they need to resume for some
        reason, check that the resource has not changed in
        the meantime. It is also cheap to compute.

        Note, since this is a strong validator/Etag, nginx will
        strip it from the headers if it has been configured with
        gzip compression for HTTP responses.

        """
        try:
            etag = None
            if self.filepath:
                mtime = os.stat(self.filepath).st_mtime
                etag = self.mtime_to_digest(mtime)
            return etag
        except Exception:
            return None


    @gen.coroutine
    def get(self, tenant: str, filename: str = None) -> None:
        """
        List the export dir, or serve a file, asynchronously.

        1. check token claims
        2. check the tenant

        If listing the dir:

        3. run the list_files method

        If serving a file:

        3. check the filename
        4. check that file exists
        5. enforce the export policy
        6. check if a byte range is being requested
        6. set the mime type
        7. serve the bytes requested (explicitly, or implicitly), chunked

        """
        try:
            self.path = self.export_dir
            resource = url_unescape(self.resource) # parsed from URI
            if not filename or os.path.isdir(f'{self.path}/{resource}'):
                if not self.allow_list:
                    raise ClientMethodNotAllowed
                if filename and os.path.isdir(f'{self.path}/{resource}'):
                    self.path += f'/{resource}'
                root = True if self.resource == self.path else False
                self.list_files(self.path, tenant, root)
                return
            if not self.allow_export:
                raise ClientMethodNotAllowed
            self.filepath = f"{self.path}/{resource}"
            if not os.path.lexists(f'{self.filepath}'):
                raise ClientResourceNotFoundError(f'{self.filepath} not found')
            size, mime_type, mtime = self.get_file_metadata(self.filepath)
            if not self.enforce_export_policy(self.export_policy, self.filepath, tenant, size, mime_type):
                raise ClientError("export policy violation")
            encrypt_data = False
            if 'Nacl-Nonce' in self.request.headers.keys():
                self.decrypt_nacl_headers(self.request.headers)
                self.CHUNK_SIZE = self.nacl_chunksize
                encrypt_data = True
            self.set_header('Content-Type', mime_type)
            self.set_header('Modified-Time', str(mtime))
            if 'Range' not in self.request.headers:
                self.set_header('Content-Length', size)
                self.flush()
                fd = open(self.filepath, "rb")
                data = fd.read(self.CHUNK_SIZE)
                while data:
                    if encrypt_data:
                        data = libnacl.crypto_stream_xor(
                            data,
                            self.nacl_nonce,
                            self.nacl_key
                        )
                    self.write(data)
                    yield self.flush()
                    data = fd.read(self.CHUNK_SIZE)
                fd.close()
            elif 'Range' in self.request.headers:
                if 'If-Range' in self.request.headers:
                    provided_etag = self.request.headers['If-Range']
                    computed_etag = self.compute_etag()
                    if provided_etag != computed_etag:
                        raise ClientError(
                            'The resource has changed, get everything from the start again'
                        )
                # clients specify the range in terms of 0-based index numbers
                # with an inclusive interval: [start, end]
                client_byte_index_range = self.request.headers['Range']
                full_file_size = os.stat(self.filepath).st_size
                start_and_end = client_byte_index_range.split('=')[-1].split('-')
                if ',' in start_and_end:
                    raise ClientMethodNotAllowed('Multipart byte range requests not supported')
                client_start = int(start_and_end[0])
                cursor_start = client_start
                try:
                    client_end = int(start_and_end[1])
                except Exception as e:
                    client_end = full_file_size - 1
                if client_end > full_file_size:
                    raise ClientContentRangeError('Range request exceeds byte range of resource')
                # because clients provide 0-based byte indices
                # we must add 1 to calculate the desired amount to read
                bytes_to_read = client_end - client_start + 1
                self.set_header('Content-Length', bytes_to_read)
                self.flush()
                fd = open(self.filepath, "rb")
                fd.seek(cursor_start)
                sent = 0
                if self.CHUNK_SIZE > bytes_to_read:
                    self.CHUNK_SIZE = bytes_to_read
                data = fd.read(self.CHUNK_SIZE)
                if encrypt_data:
                    data = libnacl.crypto_stream_xor(
                        data,
                        self.nacl_nonce,
                        self.nacl_key
                    )
                while data and sent <= bytes_to_read:
                    self.write(data)
                    yield self.flush()
                    data = fd.read(self.CHUNK_SIZE)
                    sent = sent + self.CHUNK_SIZE
                fd.close()
            logging.info(f'user: {self.requestor}, exported file: {self.filepath}, MIME type: {mime_type}')
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
        finally:
            try:
                fd.close()
            except (OSError, UnboundLocalError) as e:
                pass
            self.finish()


    def head(self, tenant: str, filename: str) -> None:
        """
        Return information about a specific file.

        """
        try:
            if not self.allow_info:
                raise ClientMethodNotAllowed
            if not filename:
                raise ClientError('No resource specified')
            self.path = self.export_dir
            self.filepath = f"{self.path}/{url_unescape(self.resource)}"
            if not os.path.lexists(self.filepath):
                raise ClientResourceNotFoundError(f"{self.filepath} not found")
            size, mime_type, mtime = self.get_file_metadata(self.filepath)
            logging.info(f'user: {self.requestor}, checked file: {self.filepath} , MIME type: {mime_type}')
            self.set_header('Content-Length', size)
            self.set_header('Accept-Ranges', 'bytes')
            self.set_header('Content-Type', mime_type)
            self.set_header('Modified-Time', str(mtime))
            self.set_status(HTTPStatus.OK.value)
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()


    def delete(self, tenant: str, filename: str = '') -> None:
        try:
            if not self.allow_delete:
                raise ClientMethodNotAllowed
            if not filename:
                raise ClientError("No resource specified")
            self.path = self.export_dir
            self.filepath = f"{self.path}/{url_unescape(self.resource)}"
            if not os.path.lexists(self.filepath):
                raise ClientResourceNotFoundError(f'{self.filepath} not found')
            if os.path.isdir(self.filepath):
                shutil.rmtree(self.filepath)
            else:
                if self.has_posix_ownership:
                    # Allow the file to be deleted by changing the rights temporary of the parent directory
                    subprocess.call(['sudo', 'chmod', 'o+w', os.path.dirname(self.filepath)])
                    os.remove(self.filepath)
                    # Restoring the rights of the parent directory
                    subprocess.call(['sudo', 'chmod', 'o-w', os.path.dirname(self.filepath)])
                else:
                    os.remove(self.filepath)
            logging.info(f'user: {self.requestor}, deleted file: {self.filepath}')
            self.set_status(HTTPStatus.OK.value)
            self.write({'message': f'deleted {self.filepath}'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()


    def on_finish(self) -> None:
        resource_created = (
            self.request.method == 'PUT'
            or (self.request.method == 'PATCH' and self.chunk_num == 'end')
        )
        if (
            not options.maintenance_mode_enabled
            and self._status_code < 300
            and (
                self.request.method in ('GET', 'HEAD', 'DELETE')
                or resource_created
            )
        ):
            try:
                if resource_created:
                    try:
                        # switch path variables back
                        if not self.completed_resumable_file:
                            path, path_part = self.path_part, self.path
                        else:
                            path = self.completed_resumable_filename
                        resource_path = move_data_to_folder(path, self.resource_dir)
                        client_mtime = self.request.headers.get('Modified-Time')
                        if client_mtime and client_mtime != 'None':
                            set_mtime(resource_path, float(client_mtime))
                    except Exception as e:
                        logging.info('could not move data to destination folder')
                        logging.info(e)
                    try:
                        if self.request_hook['enabled']:
                            call_request_hook(
                                self.request_hook['path'],
                                [resource_path, self.requestor, options.api_user, self.group_name],
                                as_sudo=self.request_hook['sudo']
                            )
                    except Exception as e:
                        logging.info('problem calling request hook')
                        logging.info(e)
                message_data = {
                    'path': resource_path if resource_created else None,
                    'requestor': self.requestor,
                    'group': self.group_name if resource_created else None,
                }
                self.handle_mq_publication(
                    mq_config=self.mq_config,
                    data=message_data,
                )
                if not self.listing_dir:
                    self.update_request_log(
                        tenant=self.tenant,
                        backend=self.backend,
                        requestor=self.requestor,
                        method=self.request.method,
                        uri=self.request.uri,
                        app=self.get_app_name(self.request.uri),
                        claims=self.claims,
                    )
            except Exception as e:
                logging.error(e)
            self.on_finish_called = True

    def on_connection_close(self) -> None:
        """
        Called when clients close the connection.

        1. Close open file, move it to destination

        """
        if self.on_finish_called:
            return

        if not self.target_file:
            return

        if not self.target_file.closed:
            self.target_file.close()
            path = self.path
            resource_created = (
                self.request.method == 'PUT' or (
                    self.request.method == 'PATCH' and
                    self.chunk_num == 'end'
                )
            )
            if resource_created:
                resource_path = move_data_to_folder(path, self.resource_dir)
                client_mtime = self.request.headers.get('Modified-Time')
                if client_mtime and client_mtime != 'None':
                    set_mtime(resource_path, float(client_mtime))
                if self.request_hook['enabled']:
                    call_request_hook(
                        self.request_hook['path'],
                        [resource_path, self.requestor, options.api_user, self.group_name],
                        as_sudo=self.request_hook['sudo']
                    )
            # otherwise leave the partial upload in place, as is
            # most likely a client that closed the connection
            # while uploading a chunk, that was never finished
            message_data = {
                    'path': resource_path if resource_created else None,
                    'requestor': self.requestor,
                    'group': self.group_name if resource_created else None,
                }
            self.handle_mq_publication(
                mq_config=self.mq_config,
                data=message_data,
            )
            self.update_request_log(
                tenant=self.tenant,
                backend=self.backend,
                requestor=self.requestor,
                method=self.request.method,
                uri=self.request.headers.get('Original-Uri'),
                app=self.get_app_name(self.request.uri),
                claims=self.claims,
            )

class GenericTableHandler(AuthRequestHandler):

    """
    Manage data in generic db backend.

    """

    def create_table_name(self, table_name: str) -> str:
        return table_name.replace('/', '_')

    def get_uri_query(self, uri: str) -> str:
        if '?' in uri:
            return url_unescape(uri.split('?')[-1])
        else:
            return ''

    def initialize(self, backend: str) -> None:
        self.backend = backend
        self.tenant = tenant_from_url(self.request.uri)
        self.endpoint = self.request.uri.split('/')[3]
        self.backend_config = options.config['backends']['dbs'][backend]
        self.dbtype = self.backend_config['db']['engine']
        self.table_structure = self.backend_config['table_structure']
        self.mq_config = self.backend_config.get('mq')
        self.check_tenant = self.backend_config.get('check_tenant')


    def prepare(self) -> Optional[Awaitable[None]]:
        try:
            if options.maintenance_mode_enabled:
                raise ServerMaintenanceError
            self.rid_info = {'key': None, 'values': []}
            self.authnz = self.process_token_and_extract_claims(
                check_tenant=self.check_tenant if self.check_tenant is not None else options.check_tenant
            )
            if self.dbtype == 'sqlite':
                self.import_dir_pattern = self.backend_config['db']['path']
                self.tenant_dir = choose_storage(
                    tenant=self.tenant,
                    endpoint_backend=self.backend,
                    opts=options,
                    directory=self.import_dir_pattern.replace(options.tenant_string_pattern, self.tenant),
                )
                if self.backend == 'apps_tables':
                    app_name = self.request.uri.split('/')[4]
                    self.db_name = f'.{self.backend}_{app_name}.db'
                else:
                    self.db_name = f'.{self.backend}.db'
                self.engine = sqlite_init(self.tenant_dir, name=self.db_name, builtin=True)
                self.db = SqliteBackend(self.engine, requestor=self.requestor)
            elif self.dbtype == 'postgres':
                if self.backend == 'apps_tables':
                    app_name = self.request.uri.split('/')[4]
                    schema = f'{self.tenant}_{app_name}'
                else:
                    schema = self.tenant
                self.db = PostgresBackend(options.pgpools.get(self.backend), schema=schema, requestor=self.requestor)
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.finish()


    def decrypt_nacl_data(self, data: bytes, headers: tornado.httputil.HTTPHeaders) -> str:
        out = b''
        nacl_stream_buffer = data
        nacl_nonce = options.sealed_box.decrypt(
            base64.b64decode(headers['Nacl-Nonce'])
        )
        nacl_key = options.sealed_box.decrypt(
            base64.b64decode(headers['Nacl-Key'])
        )
        try:
            nacl_chunksize = int(headers['Nacl-Chunksize'])
        except KeyError:
            raise ClientError('Missing Nacl-Chunksize header - cannot decrypt data')
        if nacl_chunksize > options.max_nacl_chunksize:
            raise ClientError(f'Nacl-Chunksize larger than max allowed: {options.max_nacl_chunksize}')
        while len(nacl_stream_buffer) >= nacl_chunksize:
            target_content = nacl_stream_buffer[:nacl_chunksize]
            remainder = nacl_stream_buffer[nacl_chunksize:]
            decrypted = libnacl.crypto_stream_xor(
                target_content,
                nacl_nonce,
                nacl_key
            )
            out += decrypted
            nacl_stream_buffer = remainder
        if nacl_stream_buffer:
            decrypted = libnacl.crypto_stream_xor(
                nacl_stream_buffer,
                nacl_nonce,
                nacl_key
            )
            out += decrypted
        return out.decode()

    def get_nested_data(self, keys: list, data: Union[list, dict]) -> list:
        values = []
        if isinstance(data, dict):
            target = data
            for key in keys:
                target = target.get(key)
            values.append(target)
        elif isinstance(data, list):
            for entry in data:
                target = entry
                for key in keys:
                    target = target.get(key)
                values.append(target)
        return values

    def set_resource_identifier_info(self, data: Union[list, dict]) -> None:
        """
        When publising messages to rabbitmq, it is useful
        for queue consumers to have references to the specific
        resources that were created, modified or deleted.

        In the case where all information is contained in the URI,
        such as for GET, and DELETE, this function is superfluous.

        However, for both PUT and PATCH, resource references are
        contained in the request body. Clients can optionally
        provide information about which key(s) in the request
        data contain unique references to their data, by setting
        the Resource-Identifier-Key header.

        This function will then put both the key, and the value(s)
        produced by using the key, in the payload of the rabbitmq
        message.  Downstream queue consumers can then use these
        references to fetch the target resources from the API.

        Providing the Resource-Identifier is optional in this
        scheme, while providing the Resource-Identifier-Key is
        necessary (in order for downstream data processors to
        identify new resources).

        Keys can support nesting, e.g. given data:

            {'metadata':{'id': 123}"}

        The following header will allow the API to find the value
        if not provided in the Resource-Identifier:

            Resource-Identifier-Key: metadata.id

        """
        rid_key = self.request.headers.get('Resource-Identifier-Key')
        rid = self.request.headers.get('Resource-Identifier')
        if rid_key and rid:
            self.rid_info = {'key': rid_key, 'values': [rid]}
        if rid_key and not rid: # then we look into the data
            keys = rid_key.split('.')
            rid_values = self.get_nested_data(keys, data)
            self.rid_info = {'key': rid_key, 'values': rid_values}


    @gen.coroutine
    def get(self, tenant: str, table_name: str = None) -> None:
        try:
            if not table_name:
                tables = self.db.tables_list(
                    exclude_endswith = ["_audit", "_metadata"],
                    remove_pattern = "_submissions",
                )
                self.set_status(HTTPStatus.OK.value)
                self.write({'tables': tables})
            else:
                if self.table_structure:
                    # describe sub-endpoints of tables
                    base_url = self.request.uri.split('?')[0]
                    for entry in self.table_structure:
                        if base_url.endswith(entry):
                            data_request = True
                            break
                        else:
                            data_request = False
                else:
                    data_request = True
                if not data_request:
                    self.set_status(HTTPStatus.OK.value)
                    self.write({'data': self.table_structure})
                else:
                    table_name = self.create_table_name(table_name)
                    self.set_header('Content-Type', 'application/json')
                    query = self.get_uri_query(self.request.uri)
                    # don't include metadata and audit tables
                    # when broadcasting an aggregate query with *
                    # unless the URI specifies they should be 
                    results = self.db.table_select(table_name, query, exclude_endswith = ["_audit", "_metadata"])
                    # At this point the query was created and determined valid
                    result = next(results, None)
                    self.set_status(HTTPStatus.OK.value)
                    if not result:
                       self.write('[]')
                       return
                    self.write('[')
                    self.write(json.dumps(result))
                    for row in results:
                        self.write(',')
                        self.write(json.dumps(row))
                        self.flush()
                    self.write(']')
                    self.flush()
        # TODO: raise custom exceptions from pysquril to identify query errors as 4XX
        except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError) as e:
            if table_name.endswith('_audit'):
                # Handle the audit table differently
                # it is not created until the first change appears
                self.set_status(HTTPStatus.OK.value)
                self.write('[]')
            else:
                logging.error(e)
                self.set_status(HTTPStatus.NOT_FOUND.value)
                self.write({'message': f'table {table_name} does not exist'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': error.reason})


    def put(self, tenant: str, table_name: str) -> None:
        try:
            if self.request.headers.get('Content-Type') == 'application/json+nacl':
                new_data = self.decrypt_nacl_data(
                    self.request.body,
                    self.request.headers
                )
            else:
                new_data = self.request.body
            data = json_decode(new_data)
            self.set_resource_identifier_info(data)
            table_name = self.create_table_name(table_name)
            if self.request.uri.endswith('/audit'):
                raise ClientAuthorizationError('Not allowed to write to audit tables')
            self.db.table_insert(table_name, data)
            self.set_status(HTTPStatus.CREATED.value)
            self.write({'message': 'data stored'})
        except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError) as e:
            logging.error(e)
            self.set_status(HTTPStatus.NOT_FOUND.value)
            self.write({'message': f'table {table_name} does not exist'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': error.reason})


    def patch(self, tenant: str, table_name: str) -> None:
        try:
            table_name = self.create_table_name(table_name)
            if self.request.uri.endswith('/audit'):
                raise ClientAuthorizationError('Not allowed to write to audit tables')
            if self.request.headers.get('Content-Type') == 'application/json+nacl':
                new_data = self.decrypt_nacl_data(
                    self.request.body,
                    self.request.headers
                )
            else:
                new_data = self.request.body
            data = json_decode(new_data)
            self.set_resource_identifier_info(data)
            query = self.get_uri_query(self.request.uri)
            self.db.table_update(table_name, query, data)
            self.set_status(HTTPStatus.CREATED.value)
            self.write({'data': 'data updated'})
        except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError) as e:
            logging.error(e)
            self.set_status(HTTPStatus.NOT_FOUND.value)
            self.write({'message': f'table {table_name} does not exist'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': error.reason})


    def delete(self, tenant: str, table_name: str) -> None:
        try:

            table_name = self.create_table_name(table_name)
            if self.request.uri.endswith('/audit'):
                raise ClientAuthorizationError('Not allowed to delete from audit tables')
            query = self.get_uri_query(self.request.uri)
            data = self.db.table_delete(table_name, query)
            self.set_status(HTTPStatus.OK.value)
            self.write({'data': data})
        except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError) as e:
            logging.error(e)
            self.set_status(HTTPStatus.NOT_FOUND.value)
            self.write({'message': f'table {table_name} does not exist'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': error.reason})


    def on_finish(self) -> None:
        try:
            if not options.maintenance_mode_enabled and self._status_code < 300:
                message_data = {
                    'path': None,
                    'requestor': self.requestor,
                    'group': None,
                    'resource_identifier': self.rid_info
                }
                self.handle_mq_publication(
                    mq_config=self.mq_config,
                    data=message_data
                )
                self.update_request_log(
                    tenant=self.tenant,
                    backend=self.backend,
                    requestor=self.requestor,
                    method=self.request.method,
                    uri=self.request.uri,
                    app=self.get_app_name(self.request.uri),
                    claims=self.claims,
                )
        except Exception as e:
            logging.error(e)


class HealthCheckHandler(RequestHandler):

    def head(self, tenant: str) -> None:
        self.set_status(HTTPStatus.OK.value)
        self.write({'message': 'healthy'})


class NaclKeyHander(RequestHandler):

    def get(self, tenant: str) -> None:
        public_key = options.config.get('nacl_public').get('public')
        out = {
            'public_key': public_key,
            'encoding': 'base64',
            'alg': 'sealed_box,X25519,XSalsa20-Poly1305',
            'info': 'https://libsodium.gitbook.io/doc/public-key_cryptography/sealed_boxes',
            'exp': None,
            'usage': {
                'explanation':
                    "To be used in combination with encrypted stream " +
                     "(https://libsodium.gitbook.io/doc/secret-key_cryptography/secretstream) - " +
                     "clients are required to use the public_key to encrypt their secret key " +
                     "and nonce, and send this in the Nacl-Key, and Nacl-Nonce headers, along with the payload." +
                     "For more information see endpoint-specific docs.",
                'secret_stream_headers': {
                    'Nacl-Nonce': 'base64 encoded xchacha20poly1305 nonce',
                    'Nacl-Key': 'base64 encoded xchacha20poly1305 key',
                    'Nacl-Chunksize': 'string value: size of encrypted chunks, in bytes'
                },
                'max_bytes': {
                    'Nacl-Chunksize': options.max_nacl_chunksize,
                }
            }
        }
        self.write(out)

class RunTimeConfigurationHandler(RequestHandler):

    def post(self) -> None:
        try:
            action = url_escape(self.get_query_argument('maintenance'))
            if action not in ['on', 'off']:
                raise ClientError(f'action: {action} not supported')
            if action == 'on':
                options.maintenance_mode_enabled = True
            elif action == 'off':
                options.maintenance_mode_enabled = False
            self.write({'maintenance_mode_enabled': options.maintenance_mode_enabled})
        except Exception as e:
            self.set_status(HTTPStatus.BAD_REQUEST.value)
            logging.error(e)

    def get(self) -> None:
        self.write({'maintenance_mode_enabled': options.maintenance_mode_enabled})


class AuditLogViewerHandler(AuthRequestHandler):

    def get_uri_query(self, uri: str) -> str:
        if '?' in uri:
            return url_unescape(uri.split('?')[-1])
        else:
            return ''

    def get(self, tenant: str, backend: str = None) -> None:
        if not options.request_log:
            self.set_status(HTTPStatus.SERVICE_UNAVAILABLE.value)
            self.write({'message': 'logging has not been configured'})
        try:
            if not backend:
                backends = list(options.request_log.get('backends').keys())
                available = []
                for backend in backends:
                    if backend.startswith('apps_'):
                        backend = 'apps'
                    if 'apps' in available:
                        continue
                    available.append(backend)
                self.set_status(HTTPStatus.OK.value)
                self.write({'logs': available})
            elif backend == 'apps':
                engine_type = options.request_log.get('db').get('engine')
                log_apps = self.get_log_apps(tenant, backend, engine_type)
                self.set_status(HTTPStatus.OK.value)
                self.write({'apps': log_apps})
            else:
                app = self.get_app_name(self.request.uri)
                engine_type = options.request_log.get('db').get('engine')
                db = self.get_log_db(tenant, backend, engine_type, app=app)
                self.set_status(HTTPStatus.OK.value)
                self.set_header('Content-Type', 'application/json')
                self.write('[')
                self.flush()
                first = True
                query = self.get_uri_query(self.request.uri)
                table_name = self._log_table_name(backend, app)
                for row in db.table_select(table_name, query):
                    if not first:
                        self.write(',')
                    self.write(row)
                    self.flush()
                    first = False
                self.write(']')
                self.flush()
        except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError) as e:
            logging.error(e)
            self.set_status(HTTPStatus.NOT_FOUND.value)
            self.write({'message': f'no logs available'})
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)
            self.write({'message': error.reason})


class ConfigHandler(RequestHandler):

    def get(self) -> None:
        try:
            self.write(options.config)
        except Exception as e:
            error = error_for_exception(e)
            logging.error(error.message)
            for name, value in error.headers.items():
                self.set_header(name, value)
            self.set_status(error.status, reason=error.reason)

class TestTokenHandler(RequestHandler):

    def post(self, pnum) -> None:
        secrets = gen_test_jwt_secrets(options.config)
        token = tkn(
            secrets.get(pnum),
            exp=10,
            role="import_user",
            tenant=pnum,
            user=f"{pnum}-test",
        )
        self.write({"token": token})

class Backends(object):

    default_routes = {
        'health': [
            ('/v1/(.*)/files/health', HealthCheckHandler),
        ],
        'runtime_configuration': [
            ('/v1/admin.*', RunTimeConfigurationHandler),
        ],
        'audit_log_viewer': [
            ('/v1/(.+)/logs', AuditLogViewerHandler),
            ('/v1/(.+)/logs/(.+)', AuditLogViewerHandler),
        ],
        'config': [
            ('/v1/all/config', ConfigHandler),
        ],
        'token': [
            ('/v1/(.+)/token', TestTokenHandler),
        ],
    }

    optional_routes = {
        'files_import': [
            ('/v1/(.*)/files/stream', FileRequestHandler, dict(backend='files_import', namespace='files', endpoint='stream')),
            ('/v1/(.*)/files/stream/(.*)', FileRequestHandler, dict(backend='files_import', namespace='files', endpoint='stream')),
            ('/v1/(.*)/files/resumables', ResumablesHandler, dict(backend='files_import')),
            ('/v1/(.*)/files/resumables/(.*)', ResumablesHandler, dict(backend='files_import')),
            ('/v1/(.*)/files/crypto/key', NaclKeyHander),
        ],
        'files_export': [
            ('/v1/(.*)/files/export', FileRequestHandler, dict(backend='files_export', namespace='files', endpoint='export')),
            ('/v1/(.*)/files/export/(.*)', FileRequestHandler, dict(backend='files_export', namespace='files', endpoint='export')),
        ],
        'survey': [
            ('/v1/(.*)/survey/crypto/key', NaclKeyHander),
            ('/v1/(.*)/survey/([a-zA-Z_0-9]+/attachments.*)', FileRequestHandler, dict(backend='survey', namespace='survey', endpoint=None)),
            ('/v1/(.*)/survey/resumables', ResumablesHandler, dict(backend='survey')),
            ('/v1/(.*)/survey/resumables/(.*)', ResumablesHandler, dict(backend='survey')),
            ('/v1/(.*)/survey/([a-zA-Z_0-9*]+/metadata)', GenericTableHandler, dict(backend='survey')),
            ('/v1/(.*)/survey/([a-zA-Z_0-9*]+)/submissions', GenericTableHandler, dict(backend='survey')),
            ('/v1/(.*)/survey/([a-zA-Z_0-9*]+/audit)', GenericTableHandler, dict(backend='survey')),
            ('/v1/(.*)/survey/([a-zA-Z_0-9]+)$', GenericTableHandler, dict(backend='survey')),
            ('/v1/(.*)/survey', GenericTableHandler, dict(backend='survey')),
        ],
        'form_data': [
            ('/v1/(.*)/sns/(.*)/(.*)', SnsFormDataHandler, dict(backend='sns')),
        ],
        'publication': [
            ('/v1/(.*)/publication/import', FileRequestHandler, dict(backend='publication', namespace='publication', endpoint='import')),
            ('/v1/(.*)/publication/import/(.*)', FileRequestHandler, dict(backend='publication', namespace='publication', endpoint='import')),
            ('/v1/(.*)/publication/resumables', ResumablesHandler, dict(backend='publication')),
            ('/v1/(.*)/publication/resumables/(.*)', ResumablesHandler, dict(backend='publication')),
            ('/v1/(.*)/publication/export', FileRequestHandler, dict(backend='publication', namespace='publication', endpoint='export')),
            ('/v1/(.*)/publication/export/(.*)', FileRequestHandler, dict(backend='publication', namespace='publication', endpoint='export')),
            ('/v1/(.*)/publication/tables/(.+)', GenericTableHandler, dict(backend='publication')),
            ('/v1/(.*)/publication/tables', GenericTableHandler, dict(backend='publication')),
        ],
        'apps_files': [
            ('/v1/(.*)/apps/.+/resumables', ResumablesHandler, dict(backend='apps_files')),
            ('/v1/(.*)/apps/.+/resumables/(.*)', ResumablesHandler, dict(backend='apps_files')),
            ('/v1/(.*)/apps/(.+/files.*)', FileRequestHandler, dict(backend='apps_files', namespace='apps', endpoint=None)),
        ],
        'apps_tables': [
            ('/v1/(.*)/apps/.+/tables/(.+)$', GenericTableHandler, dict(backend='apps_tables')),
            ('/v1/(.*)/apps/crypto/key', NaclKeyHander),
        ]
    }

    database_backends = {
        'sqlite': SqliteBackend,
        'postgres': PostgresBackend,
    }

    def __init__(self, config: dict) -> None:

        self.config = config
        self.routes = []
        self.exchanges = {}

        print(colored(f'tsd-file-api, listening on port {options.port}', 'yellow'))

        print(colored('Loading default routes:', 'magenta'))
        for name, route_set in self.default_routes.items():
            for route in route_set:
                print(colored(f'- {route[0]}', 'yellow'))
                self.routes.append(route)

        print(colored('Loading backend configuration:', 'magenta'))
        for backend_set in self.config['backends']:
            for backend in options.config['backends'][backend_set]:
                if backend in self.optional_routes.keys():
                    print(colored(f'Initialising: {backend}', 'cyan'))
                    for route in self.optional_routes[backend]:
                        print(colored(f'- {route[0]}', 'yellow'))
                        self.routes.append(route)

        db_backends = self.config['backends']['dbs'].items()
        if db_backends:
            define('pgpools', {})
            print(colored('Initialising database backends', 'magenta'))
            for name, backend in db_backends:
                db_backend = self.database_backends[backend['db']['engine']]
                print(colored(f"DB backend: {backend['db']['engine']}, {name}", 'cyan'))
                if db_backend.generator_class.db_init_sql:
                    self.initdb(name, options)

        if self.config.get('rabbitmq', {}).get('enabled'):
            print(colored('Finding rabbitmq exchanges', 'magenta'))
            for backend_set in self.config['backends']:
                for name, backend in options.config['backends'][backend_set].items():
                    self.find_exchanges(name, backend)

        if self.config.get('request_log'):
            print(colored('Initialising request log db', 'magenta'))
            try:
                self.initdb_request_log()
            except Exception as e:
                logging.warning(f'could not connect to request log db: {e}')

    def initdb(self, name: str, opts: tornado.options.OptionParser) -> None:
        engine_type = options.config['backends']['dbs'][name]['db']['engine']
        if engine_type == 'postgres':
            pool = postgres_init(options.config['backends']['dbs'][name]['db']['dbconfig'])
            options.pgpools[name] = pool
            db = PostgresBackend(pool)
            db.initialise()
        else:
            return

    def initdb_request_log(self) -> None:
        if not options.request_log:
            return
        engine_type = options.request_log.get('db').get('engine')
        if engine_type == 'postgres':
            pool = postgres_init(options.request_log.get('db').get('dbconfig'))
            try:
                define('pgpool_request_log', pool)
            except tornado.options.Error:
                pass # already defined ^
            logdb = PostgresBackend(pool)
            logdb.initialise()
        else: # sqlite
            return

    def find_exchanges(self, name: str, backend_config: dict) -> None:
        mq_config = backend_config.get('mq')
        if not mq_config:
            return
        print(
            colored(f'Backend: {name}, ', 'cyan'),
            colored(f'rabbitmq settings: {mq_config}', 'yellow')
        )
        existing_exchanges = []
        for name, config in self.exchanges.items():
            existing_exchanges.append(config.get('exchange'))
        if (not self.exchanges.get(name) and
            mq_config.get('exchange') not in existing_exchanges):
            self.exchanges[name] = mq_config


def main() -> None:
    tornado.log.enable_pretty_logging()
    backends = Backends(options.config)
    pika_client = (
        PikaClient(options.rabbitmq, backends.exchanges) if options.rabbitmq.get('enabled')
        else None
    )
    app = Application(
        backends.routes,
        **{'pika_client': pika_client, 'debug': options.debug}
    )
    app.listen(options.port, max_body_size=options.max_body_size)
    ioloop = IOLoop.instance()
    if pika_client:
        ioloop.add_timeout(time.time() + .1, pika_client.connect)
    if options.projects_pool:
        channel_projects = pg_listen_channel(options.projects_pool, 'channel_projects')
        ioloop.add_handler(channel_projects, handle_iam_projects_events, IOLoop.READ)
    ioloop.start()


if __name__ == '__main__':
    main()
