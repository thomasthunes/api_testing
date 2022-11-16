# -*- coding: utf-8 -*-

import errno
import os
import re
import logging
import hashlib
import subprocess
import shlex
import stat
import re
import shutil

from typing import Union, Optional

import tornado.options

from exc import (
    ClientIllegalFilenameError,
    ClientSnsPathError,
    ServerStorageTemporarilyUnavailableError,
    ServerStorageNotMountedError,
    ServerSnsError,
)

VALID_FORMID = re.compile(r'^[0-9]+$')
PGP_KEY_FINGERPRINT = re.compile(r'^[0-9A-Z]{16}$')
VALID_UUID = re.compile(r'([a-f\d0-9-]{32,36})')

def _rwxrwx___() -> int:
    u = stat.S_IREAD | stat.S_IWRITE | stat.S_IXUSR
    g = stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
    return u | g

def _rwxrws___() -> int:
    return _rwxrwx___() | stat.S_ISGID


def _find_ess_dir(pnum: str, root: str = "/ess",) -> Optional[str]:
    sub_dir = None
    for projects_dir in os.listdir(root):
        if pnum in os.listdir(f"{root}/{projects_dir}"):
            sub_dir = projects_dir
            break
    return None if not sub_dir else f"{root}/{sub_dir}/{pnum}/data/durable"


def find_tenant_storage_path(
    tenant: str,
    endpoint_backend: str,
    opts: tornado.options.OptionParser,
    root: str = "/ess",
    default_storage_backend: str = "hnas",
) -> str:
    """
    Either one of these:

        - /tsd/{pnum}/data/durable
        - /ess/projects0{1|2|3}/{pnum}/data/durable

    Results are cached in a dict stored on options:

    {
        pnum: {
            storage_backend: Optional[str] (hnas|migrating|ess),
            storage_paths: {
                hnas: str,
                ess: Optional[str],
            },
            sns: {
                hnas: bool,
                ess: bool,
            }
        },
        ...
    }

    Returns the path which the endpoint_backend should use.

    """
    cache = opts.tenant_storage_cache.copy()
    tenant_info = opts.migration_statuses.get(tenant, {})
    storage_backend = tenant_info.get("storage_backend", default_storage_backend)
    sns_ess_delivery = tenant_info.get("sns_ess_delivery", False)
    sns_loader_processing = tenant_info.get("sns_loader_processing", False)
    sns_migration_done = tenant_info.get("sns_ess_migration", False)
    # always update cache
    if not cache.get(tenant):
        cache[tenant] = {
            "storage_backend": storage_backend,
            "storage_paths": {
                "hnas": f"/tsd/{tenant}/data/durable",
                "ess": None,
            },
        }

    cache[tenant]["sns"] = {
        "hnas": True if not (sns_loader_processing or sns_migration_done) else False,
        "ess": True if (sns_ess_delivery or sns_loader_processing or sns_migration_done) else False,
    }
    # optionally look for ESS path
    if (
        storage_backend == "ess"
        and not cache.get(tenant).get("storage_paths").get("ess")
    ):
        cache[tenant]["storage_paths"]["ess"] = _find_ess_dir(tenant, root=root)
    # store updated cache
    opts.tenant_storage_cache = cache.copy()
    # use updated info from now on
    project = cache.get(tenant)
    preferred = "ess" if endpoint_backend in opts.prefer_ess else "hnas"
    current_storage_backend = project.get("storage_backend")
    # determine which path to return
    if current_storage_backend == "migrating":
        if preferred == "ess":
            raise ServerStorageTemporarilyUnavailableError
        else:
            return project.get("storage_paths").get(preferred)
    elif current_storage_backend == "ess":
        if endpoint_backend == "sns":
            if project.get("sns").get("hnas"):
                preferred = "hnas"
            elif project.get("sns").get("ess"):
                preferred = "ess"
            else:
                preferred = "hnas"
        return project.get("storage_paths").get(preferred)
    else:
        return project.get("storage_paths").get("hnas")


def choose_storage(
    *,
    tenant: str,
    endpoint_backend: str,
    opts: tornado.options.OptionParser,
    directory: str,
) -> str:
    if not directory.startswith("/tsd"):
        return directory
    split_on = "data/durable"
    storage_path = find_tenant_storage_path(
        tenant, endpoint_backend, opts,
    ).split(split_on)
    in_dir = directory.split(split_on)
    out_dir = "".join([storage_path[0], split_on, in_dir[-1]])
    return out_dir


def call_request_hook(path: str, params: list, as_sudo: bool = True) -> None:
    if as_sudo:
        cmd = ['sudo']
    else:
        cmd = []
    cmd.append(shlex.quote(path))
    cmd.extend(params)
    subprocess.call(cmd)


def tenant_from_url(url: str) -> list:
    if 'v1' in url:
        idx = 2
    else:
        idx = 1
    return url.split('/')[idx]


def check_filename(filename: str, disallowed_start_chars: list = []) -> str:
    start_char = filename[0]
    if disallowed_start_chars:
        if start_char in disallowed_start_chars:
            raise ClientIllegalFilenameError(
                f"Filename: {filename} has illegal start character: {start_char}"
            )
    return filename


def sns_dir(
    base_pattern: str,
    tenant: str,
    uri: str,
    tenant_string_pattern: str,
    test: bool = False,
    options: tornado.options.OptionParser = None,
    use_hnas: bool = True,
    use_ess: bool = False,
) -> str:
    """
    Construct and create a path for sns uploads.
    If ESS is ready, create the path there too.
    If HNAS is no longer in use, ignore it, and return the ESS path.

    """
    try:
        uri_parts = uri.split('/')
        formid = uri_parts[-1]
        keyid = uri_parts[-2]
        if not VALID_FORMID.match(formid):
            raise ClientSnsPathError(f'invalid form ID: {formid}')
        if not PGP_KEY_FINGERPRINT.match(keyid):
            raise ClientSnsPathError(f'invalid PGP fingerprint: {keyid}')
        folder = base_pattern.replace(tenant_string_pattern, tenant).replace('KEYID', keyid).replace('FORMID', formid)
        hnas_sns_dir = os.path.normpath(folder)
        if test:
            return hnas_sns_dir
        if use_hnas and not os.path.lexists(hnas_sns_dir):
            try:
                os.makedirs(hnas_sns_dir)
                os.chmod(hnas_sns_dir, _rwxrws___())
                logging.info(f'Created: {hnas_sns_dir}')
            except OSError as e:
                if e.errno == errno.ENOENT:
                    raise ServerStorageNotMountedError(f"NFS mount missing for {tenant}") from e
                else:
                    raise e
            except Exception as e:
                logging.error(e)
                logging.error(f"Could not create {hnas_sns_dir}")
                raise ServerSnsError from e
        if use_ess:
            try:
                ess_path = options.tenant_storage_cache.get(tenant).get("storage_paths").get("ess")
                ess_sns_dir = base_pattern.replace(
                    tenant_string_pattern,
                    tenant
                ).replace(
                    'KEYID',
                    keyid
                ).replace(
                    'FORMID',
                    formid
                ).replace(
                    f"/tsd/{tenant}/data/durable",
                    ess_path
                )
                if not os.path.lexists(ess_sns_dir):
                    os.makedirs(ess_sns_dir)
                    os.chmod(ess_sns_dir, _rwxrws___())
                    logging.info(f"Created: {ess_sns_dir}")
            except OSError as e:
                if e.errno == errno.ENOENT:
                    raise ServerStorageNotMountedError(f"NFS mount missing for {ess_path}") from e
                else:
                    raise e
            except Exception as e:
                logging.error(e)
                logging.error(f"Could not create {ess_sns_dir}")
                raise ServerSnsError from e
        return hnas_sns_dir if use_hnas else ess_sns_dir
    except Exception as e:
        logging.error(e)
        raise ServerSnsError from e


def md5sum(filename: str, blocksize: int = 65536) -> str:
    _hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            _hash.update(block)
    return _hash.hexdigest()


def move_data_to_folder(path: str, dest: str) -> Union[str, bool]:
    """
    Move file/dir at path into and folder at dest.

    """
    try:
        if not dest:
            return path
        filename = os.path.basename(path)
        base_dir = path.replace(f'/{filename}', '')
        new_path = os.path.normpath(dest + '/' + filename)
        if os.path.isdir(path):
            if os.path.lexists(new_path):
                # idempotency
                shutil.rmtree(new_path)
            shutil.move(path, new_path)
        else:
            os.rename(path, new_path)
        return new_path
    except Exception as e:
        logging.error(e)
        logging.error('could not move file: %s', path)
        return False

def set_mtime(path: str, mtime: int) -> None:
    mtime = mtime
    atime = mtime
    os.utime(path, (mtime, atime))
