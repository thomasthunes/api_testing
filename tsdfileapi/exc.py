
"""
Error definitions for failure modes for which we can determine the cause,
mapped to HTTP status codes.

References:
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Status
- https://httpwg.org/specs/rfc9110.html#overview.of.status.codes

"""

import errno

from collections import namedtuple
from http import HTTPStatus, client

from tornado.web import HTTPError


class ApiError(Exception):

    reason = "API error"
    status = None
    headers = {}

    def __init__(self, context: str = "API Error", headers: dict = {}) -> None:
        self.message = f"{self.status.phrase}, {self.reason}, {context}"
        self.headers = headers or self.headers


# Client errors -  HTTP 4XX range
# -------------------------------

class ClientError(ApiError):
    reason = "Client error"
    status = HTTPStatus.BAD_REQUEST

class ClientIllegalFilenameError(ClientError):
    reason = "Filename not allowed"
    status = HTTPStatus.BAD_REQUEST

class ClientSnsPathError(ClientError):
    reason = "Wrong URL path"
    status = HTTPStatus.BAD_REQUEST

class ClientAuthorizationError(ClientError):
    reason = "Client not authorized for request"
    status = HTTPStatus.FORBIDDEN

class ClientMethodNotAllowed(ClientError):
    reason = "Method not allowed"
    status = HTTPStatus.METHOD_NOT_ALLOWED

class ClientReservedResourceError(ClientError):
    reason = "Reserved resource name"
    status = HTTPStatus.BAD_REQUEST

class ClientGroupAccessError(ClientError):
    reason = "Group rights does not authorize request"
    status = HTTPStatus.FORBIDDEN

class ClientNaclChunkSizeError(ClientError):
    reason = "Chunk size too large"
    status = HTTPStatus.BAD_REQUEST

class ClientResourceNotFoundError(ClientError):
    reason = "Resource not found"
    status = HTTPStatus.NOT_FOUND

class ClientContentRangeError(ClientError):
    reason = "Range not satisfiable"
    status = HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE


# Server errors - HTTP 5XX range
# ------------------------------

class ServerError(ApiError):
    reason = "Server error"
    status = HTTPStatus.INTERNAL_SERVER_ERROR

class ServerStorageTemporarilyUnavailableError(ServerError):
    reason = "Project Storage Migrating"
    status = HTTPStatus.SERVICE_UNAVAILABLE
    headers = {"X-Project-Storage": "Migrating"}

class ServerStorageNotMountedError(ServerError):
    reason = "NFS mount issue"
    status = HTTPStatus.INTERNAL_SERVER_ERROR

class ServerSnsError(ServerError):
    reason = "Issue contructing storage path"
    status = HTTPStatus.INTERNAL_SERVER_ERROR

class ServerDiskQuotaExceededError(ServerError):
    reason = "Project has run out of disk quota"
    status = HTTPStatus.INSUFFICIENT_STORAGE

class ServerMaintenanceError(ServerError):
    reason = "Server down for maintenance"
    status = HTTPStatus.SERVICE_UNAVAILABLE


# helper functions
# ----------------

Error = namedtuple(
    "Error",
    ["status", "reason", "message", "headers"],
)

def error_for_exception(exc: Exception) -> Error:
    """
    Return an Error, with information about:

    - which HTTP status code to send
    - the reason for the error
    - an informative log message
    - optionally, headers to send with the error

    This covers four cases of exceptions:

    1) defined in this module
    2) raised by the tornado framework
    3) with an errno
    4) everything else

    errno notes:

    - [Errno 2]
        - FileNotFoundError
        - errno.ENOENT
        - No such file or directory
            - NFS mount issue
            - directory not present

    - [Errno 122]
        - errno.EDQUOT
        - Disk quota exceeded

    """
    if isinstance(exc, ApiError):
        status = exc.status.value
        reason = exc.reason
        message = exc.message
        headers = exc.headers
    elif isinstance(exc, HTTPError):
        status = exc.status_code
        reason = exc.log_message
        message = f"{client.responses.get(status)}, {exc.log_message}"
        headers = {}
    elif hasattr(exc, "errno") and exc.errno == errno.EDQUOT:
        code = HTTPStatus.INSUFFICIENT_STORAGE
        status = cade.value
        reason = "Project has run out of disk quota"
        message = f"{code.phrase}, {reason}"
        headers = {}
    else:
        default = HTTPStatus.INTERNAL_SERVER_ERROR
        status = default.value
        reason = default.phrase
        message = f"{default.phrase}, {exc}"
        headers = {}
    return Error(status, reason, message, headers)
