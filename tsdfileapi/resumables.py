
import functools
import hashlib
import io
import logging
import os
import re
import shutil
import stat
import sqlite3
import uuid

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import ContextManager, Union, Optional

import sqlalchemy

from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, IntegrityError, StatementError


_IS_VALID_UUID = re.compile(r'([a-f\d0-9-]{32,36})')
_RW______ = stat.S_IREAD | stat.S_IWRITE


class ResumableNotFoundError(Exception):
    pass

class ResumableIncorrectChunkOrderError(Exception):
    pass

def _atoi(text: str) -> Union[int, str]:
    return int(text) if text.isdigit() else text


def _natural_keys(text: str) -> list:
    """
    alist.sort(key=_natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    """
    return [ _atoi(c) for c in re.split(r'(\d+)', text) ]


def _resumables_cmp(a: tuple, b: tuple) -> int:
    a_time = a[0]
    b_time = b[0]
    if a_time > b_time:
        return -1
    elif a_time < b_time:
        return 1
    else:
        return 1


def db_init(
    path: str,
    name: str = 'api-data.db',
    builtin: bool = False,
) -> Union[sqlalchemy.engine.Engine, sqlite3.Connection]:
    dbname = name
    if not builtin:
        dburl = 'sqlite:///' + path + '/' + dbname
        engine = create_engine(dburl, poolclass=QueuePool)
    else:
        engine = sqlite3.connect(path + '/' + dbname)
    return engine


@contextmanager
def session_scope(
    engine: sqlalchemy.engine.Engine,
) -> ContextManager[sqlalchemy.orm.session.Session]:
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except (OperationalError, IntegrityError, StatementError) as e:
        session.rollback()
        raise e
    finally:
        session.close()


def md5sum(filename: str, blocksize: int = 65536) -> str:
    _hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            _hash.update(block)
    return _hash.hexdigest()


class AbstractResumable(ABC):

    def __init__(self, work_dir: str = None, owner: str = None) -> None:
        super(AbstractResumable, self).__init__()
        self.work_dir = work_dir
        self.owner = owner

    @abstractmethod
    def prepare(
        self,
        work_dir: str,
        in_filename: str,
        url_chunk_num: str,
        url_upload_id: str,
        url_group: str,
        owner: str,
        key: str = None,
    ) -> tuple:
        raise NotImplementedError

    @abstractmethod
    def open_file(self, filename: str, mode: str) -> io.BufferedRandom:
        raise NotImplementedError

    @abstractmethod
    def add_chunk(self, fd: io.BufferedRandom, chunk: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def close_file(self, filename: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def merge_chunk(
        self,
        work_dir: str,
        last_chunk_filename: str,
        upload_id: str,
        owner: str,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def finalise(
        self,
        work_dir: str,
        last_chunk_filename: str,
        upload_id: str,
        owner: str,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def list_all(
        self,
        work_dir: str,
        owner: str,
        key: str = None,
    ) -> dict:
        raise NotImplementedError

    @abstractmethod
    def info(
        self,
        work_dir: str,
        filename: str,
        upload_id: str,
        owner: str,
        key: str = None,
    ) -> dict:
        raise NotImplementedError

    @abstractmethod
    def delete(
        self,
        work_dir: str,
        filename: str,
        upload_id: str,
        owner: str,
    ) -> bool:
        raise NotImplementedError


class SerialResumable(AbstractResumable):

    """
    Class for creating files in a piecemeal fashion,
    useful for resumable uploads, for example.

    The following public methods are exposed:

        a) for creating files incrementally:

            prepare
            open_file
            add_chunk
            close_file
            merge_chunk
            finalise

        b) for managing files which are still being finalised:

            list_all
            info
            delete

    """

    def __init__(self, work_dir: str = None, owner: str = None) -> None:
        super(SerialResumable, self).__init__(work_dir, owner)
        self.work_dir = work_dir
        self.owner = owner
        self.engine = self._init_db(owner, work_dir)

    def _init_db(
        self,
        owner: str,
        work_dir: str,
    ) -> Union[sqlalchemy.engine.Engine, sqlite3.Connection]:
        dbname = '{0}{1}{2}'.format('.resumables-', owner, '.db')
        rdb = db_init(work_dir, name=dbname)
        db_path = '{0}/{1}'.format(work_dir, dbname)
        if os.path.lexists(db_path):
            os.chmod(db_path, _RW______)
        return rdb

    def prepare(
        self,
        work_dir: str,
        in_filename: str,
        url_chunk_num: str,
        url_upload_id: str,
        url_group: str,
        owner: str,
        key: str = None,
    ) -> tuple:
        """
        The following cases are handled:

        1. First chunk
            - check that the chunk has not already been uploaded
            - a new upload id is generated
            - the upload id is recorded as beloning to the authenticated owner
            - a new working directory is created
            - set completed_resumable_file to None

        2. Rest of the chunks
            - ensure monotonically increasing chunk order
            - set completed_resumable_file to None

        3. End request
            - set completed_resumable_file to True

        In all cases the function returns:
        upload_id/filename.extention.chunk.num

        """
        chunk_num = int(url_chunk_num) if url_chunk_num != 'end' else url_chunk_num
        upload_id = str(uuid.uuid4()) if not url_upload_id else url_upload_id
        chunk_filename = in_filename + '.chunk.' + url_chunk_num
        filename = upload_id + '/' + chunk_filename
        if chunk_num == 'end':
            completed_resumable_file = True
            chunk_order_correct = True
        elif chunk_num == 1:
            os.makedirs(work_dir + '/' + upload_id)
            assert self._db_insert_new_for_owner(upload_id, url_group, key=key)
            chunk_order_correct = True
            completed_resumable_file = None
        elif chunk_num > 1:
            chunk_order_correct = self._refuse_upload_if_not_in_sequential_order(work_dir, upload_id, chunk_num)
            completed_resumable_file = None
        return chunk_num, upload_id, completed_resumable_file, chunk_order_correct, filename

    def open_file(self, filename: str, mode: str) -> io.BufferedRandom:
        fd = open(filename, mode)
        os.chmod(filename, _RW______)
        return fd

    def add_chunk(self, fd: io.BufferedRandom, chunk: bytes) -> None:
        if not fd:
            return
        else:
            fd.write(chunk)

    def close_file(self, fd: str) -> None:
        if fd:
            fd.close()

    def _refuse_upload_if_not_in_sequential_order(
        self,
        work_dir: str,
        upload_id: str,
        chunk_num: int,
    ) -> bool:
        chunk_order_correct = True
        full_chunks_on_disk = self._get_full_chunks_on_disk(work_dir, upload_id)
        previous_chunk_num = int(full_chunks_on_disk[-1].split('.chunk.')[-1])
        if chunk_num <= previous_chunk_num or (chunk_num - previous_chunk_num) >= 2:
            chunk_order_correct = False
            logging.error('chunks must be uploaded in sequential order')
        return chunk_order_correct

    def _find_nth_chunk(
        self,
        work_dir: str,
        upload_id: str,
        filename: str,
        n: int,
    ) -> str:
        n = n - 1 # chunk numbers start at 1, but keep 0-based for the signaure
        current_resumable = '%s/%s' % (work_dir, upload_id)
        files = os.listdir(current_resumable)
        files.sort(key=_natural_keys)
        completed_chunks = [ f for f in files if '.part' not in f ]
        out = completed_chunks[n] if completed_chunks else ''
        return out

    def _find_relevant_resumable_dir(
        self,
        work_dir: str,
        filename: str,
        upload_id: str,
        key: str = None,
    ) -> str:
        """
        If the client provides an upload_id, then the exact folder is returned.
        If no upload_id is provided, e.g. when the upload_id is lost, then
        the server will try to find a match, based on the filename, returning
        the most recent entry.

        Returns
        -------
        str, upload_id (name of the directory)

        """
        relevant = None
        potential_resumables = self._db_get_all_resumable_ids_for_owner(key=key)
        if not upload_id:
            logging.info('Trying to find a matching resumable for %s', filename)
            candidates = []
            for item in potential_resumables:
                pr = item[0]
                current_pr = '%s/%s' % (work_dir, pr)
                if _IS_VALID_UUID.match(pr) and os.path.lexists(current_pr):
                    candidates.append((os.stat(current_pr).st_mtime, pr))
            candidates = sorted(candidates, key=functools.cmp_to_key(_resumables_cmp))
            for cand in candidates:
                upload_id = cand[1]
                first_chunk = self._find_nth_chunk(work_dir, upload_id, filename, 1)
                if filename in first_chunk:
                    relevant = cand[1]
                    break
        else:
            for item in potential_resumables:
                pr = item[0]
                current_pr = '%s/%s' % (work_dir, pr)
                if _IS_VALID_UUID.match(pr) and str(upload_id) == str(pr):
                    relevant = pr
        return relevant

    def list_all(
        self,
        work_dir: str,
        owner: str,
        key: str = None,
    ) -> dict:
        potential_resumables = self._db_get_all_resumable_ids_for_owner(key=key)
        resumables = []
        info = []
        for item in potential_resumables:
            chunk_size = None
            pr = item[0]
            current_pr = '%s/%s' % (work_dir, pr)
            if _IS_VALID_UUID.match(pr):
                try:
                    chunk_size, max_chunk, md5sum, \
                    previous_offset, next_offset, \
                    warning, recommendation, \
                    filename = self._get_resumable_chunk_info(current_pr, work_dir)
                    if recommendation == 'end':
                        next_offset = 'end'
                except (OSError, Exception):
                    pass
                if chunk_size:
                    try:
                        group = self._db_get_group(pr)
                    except Exception as e:
                        group = None
                    key = None
                    try:
                        key = self._db_get_key(pr)
                    except Exception: # for transition
                        pass
                    info.append(
                        {
                            'chunk_size': chunk_size,
                            'max_chunk': max_chunk,
                            'md5sum': md5sum,
                            'previous_offset': previous_offset,
                            'next_offset': next_offset,
                            'id': pr,
                            'filename': filename,
                            'group': group,
                            'key': key
                        }
                    )
        return {'resumables': info}

    def _repair_inconsistent_resumable(
        self,
        merged_file: str,
        chunks: list,
        merged_file_size: int,
        sum_chunks_size: int,
    ) -> tuple:
        """
        If the server process crashed after a chunk was uploaded,
        but while a merge was taking place, it is likey that
        the merged file will be smaller than the sum of the chunks.

        In that case, we try to re-merge the last chunk into the file
        and return the resumable info after that. If the merged file
        is _larger_ than the sum of the chunks, then a merge has taken
        place more than once, and it is best for the client to either
        end or delete the upload. If nothing can be done then the client
        is encouraged to end the upload.

        """
        logging.info('current merged file size: %d, current sum of chunks in db %d', merged_file_size, sum_chunks_size)
        if len(chunks) == 0:
            return False
        else:
            last_chunk = chunks[-1]
            last_chunk_size = os.stat(last_chunk).st_size
        if merged_file_size == sum_chunks_size:
            logging.info('server-side data consistent')
            return chunks
        try:
            warning = None
            recommendation = None
            diff = sum_chunks_size - merged_file_size
            if (merged_file_size < sum_chunks_size) and (diff <= last_chunk_size):
                target_size = sum_chunks_size - last_chunk_size
                with open(merged_file, 'ab') as f:
                    f.truncate(target_size)
                with open(merged_file, 'ab') as fout:
                    with open(last_chunk, 'rb') as fin:
                        shutil.copyfileobj(fin, fout)
                new_merged_size = os.stat(merged_file).st_size
                logging.info('merged file after repair: %d sum of chunks: %d', new_merged_size, sum_chunks_size)
                if new_merged_size == sum_chunks_size:
                    return chunks, warning, recommendation
                else:
                    raise Exception('could not repair data')
        except (Exception, OSError) as e:
            logging.error(e)
            return chunks, 'not sure what to do', 'end'

    def _get_resumable_chunk_info(self, resumable_dir: str, work_dir: str) -> tuple:
        """
        Get information needed to resume an upload.
        If the server-side data is inconsistent, then
        we try to fix it by successively dropping the last
        chunk and truncating the merged file.

        Returns
        -------
        tuple, (size, chunknum, md5sum, previous_offset, next_offset, key)

        """
        def info(chunks: list, recommendation: str = None, warning: str = None) -> tuple:
            num = int(chunks[-1].split('.')[-1])
            latest_size = _bytes(chunks[-1])
            upload_id = os.path.basename(resumable_dir)
            next_offset = self._db_get_total_size(upload_id)
            previous_offset = next_offset - latest_size
            filename = os.path.basename(chunks[-1].split('.chunk')[0])
            merged_file = os.path.normpath(work_dir + '/' + filename + '.' + upload_id)
            try:
                # check that the size of the merge file
                # matches what we calculate from the
                # chunks recorded in the resumable db
                assert _bytes(merged_file) == next_offset
            except AssertionError:
                try:
                    logging.info('trying to repair inconsistent data')
                    chunks, warning, recommendation = self._repair_inconsistent_resumable(
                        merged_file, chunks, _bytes(merged_file), next_offset,
                    )
                    return info(chunks)
                except Exception as e:
                    logging.error(e)
                    return None, None, None, None, None, None, None, None
            return latest_size, num, md5sum(chunks[-1]), \
                   previous_offset, next_offset, recommendation, \
                   warning, filename
        def _bytes(chunk: str) -> int:
            size = os.stat(chunk).st_size
            return size
        # may contain partial files, due to failed requests
        all_chunks = [ '%s/%s' % (resumable_dir, i) for i in os.listdir(resumable_dir) ]
        all_chunks.sort(key=_natural_keys)
        chunks = [ c for c in all_chunks if '.part' not in c ]
        return info(chunks)

    def info(
        self,
        work_dir: str,
        filename: str,
        upload_id: str,
        owner: str,
        key: str = None,
    ) -> dict:
        relevant_dir = self._find_relevant_resumable_dir(work_dir, filename, upload_id, key=key)
        if not relevant_dir:
            logging.error('No resumable found for: %s', filename)
            raise ResumableNotFoundError
        resumable_dir = '%s/%s' % (work_dir, relevant_dir)
        chunk_size, \
        max_chunk, \
        md5sum, \
        previous_offset, \
        next_offset, \
        warning, \
        recommendation, \
        filename = self._get_resumable_chunk_info(resumable_dir, work_dir)
        identifier = upload_id if upload_id else relevant_dir
        try:
            group = self._db_get_group(identifier)
        except Exception as e:
            group = None
        try:
            key = self._db_get_key(identifier)
        except Exception: # for transition
            key = None
        if recommendation == 'end':
            next_offset = 'end'
        info = {
            'filename': filename,
            'id': relevant_dir,
            'chunk_size': chunk_size,
            'max_chunk': max_chunk,
            'md5sum': md5sum,
            'previous_offset': previous_offset,
            'next_offset': next_offset,
            'warning': warning,
            'group': group,
            'key': key
        }
        return info

    def _get_full_chunks_on_disk(self, work_dir: str, upload_id: str) -> list:
        chunks_on_disk = os.listdir(work_dir + '/' + upload_id)
        chunks_on_disk.sort(key=_natural_keys)
        full_chunks_on_disk = [
            c for c in chunks_on_disk if (
                '.part' not in c
                and '.chunk' in c
            )
        ]
        return full_chunks_on_disk

    def delete(
        self,
        work_dir: str,
        filename: str,
        upload_id: str,
        owner: str,
    ) -> bool:
        try:
            assert self._db_upload_belongs_to_owner(upload_id), 'upload does not belong to user'
            relevant_dir = work_dir + '/' + upload_id
            relevant_merged_file = work_dir + '/' + filename + '.' + upload_id
            shutil.rmtree(relevant_dir)
            os.remove(relevant_merged_file)
            assert self._db_remove_completed_for_owner(upload_id), 'could not remove data from resumables db'
            return True
        except (Exception, AssertionError) as e:
            logging.error(e)
            logging.error('could not complete resumable deletion')
            return False

    def finalise(
        self,
        work_dir: str,
        last_chunk_filename: str,
        upload_id: str,
        owner: str,
    ) -> str:
        assert '.part' not in last_chunk_filename
        filename = os.path.basename(last_chunk_filename.split('.chunk')[0])
        out = os.path.normpath(work_dir + '/' + filename + '.' + upload_id)
        final = out.replace('.' + upload_id, '')
        chunks_dir = work_dir + '/' + upload_id
        if '.chunk.end' in last_chunk_filename:
            logging.info('deleting: %s', chunks_dir)
            try:
                os.rename(out, final)
            except FileNotFoundError as e:
                logging.error(e)
                raise ResumableNotFoundError
            try:
                shutil.rmtree(chunks_dir) # do not need to fail upload if this does not work
            except OSError as e:
                logging.error(e)
            assert self._db_remove_completed_for_owner(upload_id)
        else:
            logging.error('finalise called on non-end chunk')
        return final

    def merge_chunk(
        self,
        work_dir: str,
        last_chunk_filename: str,
        upload_id: str,
        owner: str,
    ) -> str:
        """
        Merge chunks into one file, _in order_.

        Sequence
        --------
        1. Check that the chunk is not partial
        2. If last request
            - remove any remaining chunks, and the working directory
            - continue to the chowner: move file, set permissions
        3. If new chunk
            - if chunk_num > 1, create a lockfile - link to a unique file (NFS-safe method)
            - append it to the merge file
            - remove chunks older than 5 requests back in the sequence
              to avoid using lots of disk space for very large files
            - update the resumable's info table
        4. If a merge fails
            - remove the chunk
            - reset the file to its prior size
            - end the request
        5. Finally
            - unlink any existing lock

        Note
        ----
        This will produce bizarre files if clients send chunks out of order,
        which rules out multi-threaded senders. That can be supported by delaying
        the merge until the final request. Until a feature request arrives,
        it remain unimplemented.

        Note
        ----
        If removing a chunk with the `os.remove(chunk)` call fails, we've got
        ourselves a case of an invalid/stale chunk file. We can't remove the file,
        obviously -- `os.remove` did fail, after all. We have now a mandatory
        choice to make, between returning success and returning an error to the
        client. Returning success would be dangerous because the error we're dealing
        with that prompted us to try remove the chunk implies the chunk may be
        invalid due to not having been written entirely or something else that went
        wrong that caused the particular except block to apply. So we assume an
        error must be returned to the client. We shouldn't return a _client_ error
        code, because the client isn't at fault here -- something went wrong on our
        side of things. Assuming thus that a _server_ error is returned, since a
        client may rightfully assume some _intermittent_ server error condition,
        they may be inclined to retry the request. The environment certainly helps
        make this an attractive error handling strategy for us -- what with sporadic
        NFS I/O issues and other things that may crop up from one request to the
        next. However, since the removal of the [stale] chunk file would have failed
        during the first request, the retried request will fail the second time, but
        now with the API deeming it a _client_ error -- because the API assumes that
        if a chunk file is present then the chunk already is stored and repeating
        with the same chunk number is a clear case of chunk number order violation,
        something the API would rightfully attribute as cause to the client.

        """
        assert '.part' not in last_chunk_filename
        filename = os.path.basename(last_chunk_filename.split('.chunk')[0])
        out = os.path.normpath(work_dir + '/' + filename + '.' + upload_id)
        out_lock = out + '.lock'
        final = out.replace('.' + upload_id, '')
        chunks_dir = work_dir + '/' + upload_id
        chunk_num = int(last_chunk_filename.split('.chunk.')[-1])
        chunk = chunks_dir + '/' + last_chunk_filename
        try:
            if chunk_num > 1:
                os.link(out, out_lock)
            with open(out, 'ab') as fout:
                with open(chunk, 'rb') as fin:
                    size_before_merge = os.stat(out).st_size
                    shutil.copyfileobj(fin, fout)
            chunk_size = os.stat(chunk).st_size
            assert self._db_update_with_chunk_info(upload_id, chunk_num, chunk_size)
        except Exception as e:
            logging.error(e)
            os.remove(chunk)
            with open(out, 'ab') as fout:
                fout.truncate(size_before_merge)
            raise e
        finally:
            if chunk_num > 1:
                try:
                    os.unlink(out_lock)
                except Exception as e:
                    logging.exception(e)
        if chunk_num >= 5:
            target_chunk_num = chunk_num - 4
            old_chunk = chunk.replace('.chunk.' + str(chunk_num), '.chunk.' + str(target_chunk_num))
            try:
                os.remove(old_chunk)
            except Exception as e:
                logging.error(e)
        return final

    def _db_insert_new_for_owner(
        self,
        resumable_id: str,
        group: str,
        key: str = None,
    ) -> bool:
        """
        A backwards incompatible change introduced the key column
        to the resumable_uploads table. This is why existing tables'
        columns are altered.
        """
        resumable_accounting_table = 'resumable_uploads'
        resumable_table = f'resumable_{resumable_id}'
        with session_scope(self.engine) as session:
            resumables_table_exists = False
            current_tables = session.execute(
                "select name FROM sqlite_master where type = 'table'"
            ).fetchall()
            if len(current_tables) == 0:
                pass
            else:
                for table in current_tables:
                    if resumable_accounting_table in table[0]:
                        resumables_table_exists = True
                        break
            if not resumables_table_exists:
                session.execute(f"""
                    create table if not exists {resumable_accounting_table}(
                        id text,
                        upload_group text,
                        key text
                    )"""
                )
            else:
                try:
                    session.execute(f"""
                        alter table {resumable_accounting_table} add column key text"""
                    )
                except OperationalError as e:
                    pass # ^ already altered the table before
            session.execute(f"""
                insert into {resumable_accounting_table} (id, upload_group, key)
                values (:resumable_id, :upload_group, :key)""",
                {
                    'resumable_id': resumable_id,
                    'upload_group': group,
                    'key': key
                }
            )
            session.execute(f"""
                create table "{resumable_table}"(chunk_num int, chunk_size int)"""
            )
        return True

    def _db_update_with_chunk_info(
        self,
        resumable_id: str,
        chunk_num: int,
        chunk_size: int,
    ) -> bool:
        resumable_table = f'resumable_{resumable_id}'
        with session_scope(self.engine) as session:
            session.execute(f"""
                insert into "{resumable_table}"(chunk_num, chunk_size)
                values (:chunk_num, :chunk_size)""",
                {'chunk_num': chunk_num, 'chunk_size': chunk_size}
            )
        return True

    def _db_pop_chunk(self, resumable_id: str, chunk_num: int) -> bool:
        resumable_table = f'resumable_{resumable_id}'
        with session_scope(self.engine) as session:
            res = session.execute(f"""
                delete from "{resumable_table}"
                where chunk_num = :chunk_num""",
                {'chunk_num': chunk_num}
            )
        return True

    def _db_get_total_size(self, resumable_id: str) -> int:
        resumable_table = f'resumable_{resumable_id}'
        with session_scope(self.engine) as session:
            res = session.execute(
                f'select sum(chunk_size) from "{resumable_table}"'
            ).fetchone()[0]
        return res

    def _db_get_group(self, resumable_id: str) -> str:
        with session_scope(self.engine) as session:
            res = session.execute(
                'select upload_group from resumable_uploads where id = :resumable_id',
                {'resumable_id': resumable_id}
            ).fetchone()[0]
        return res

    def _db_get_key(self, resumable_id:  str) -> str:
        with session_scope(self.engine) as session:
            res = session.execute(
                'select key from resumable_uploads where id = :resumable_id',
                {'resumable_id': resumable_id}
            ).fetchone()[0]
        return res

    def _db_upload_belongs_to_owner(self, resumable_id: str) -> bool:
        with session_scope(self.engine) as session:
            res = session.execute(
                'select count(1) from resumable_uploads where id = :resumable_id',
                {'resumable_id': resumable_id}
            ).fetchone()[0]
        return True if res > 0 else False

    def _db_get_all_resumable_ids_for_owner(self, key: str = None) -> list:
        try:
            params = {}
            if key:
                query = 'select id from resumable_uploads where key = :key'
                params['key'] = key
            else:
                query = 'select id from resumable_uploads'
            with session_scope(self.engine) as session:
                res = session.execute(query, params).fetchall()
        except Exception:
            return []
        return res # [(id,), (id,)]

    def _db_remove_completed_for_owner(self, resumable_id: str) -> bool:
        resumable_table = f'resumable_{resumable_id}'
        with session_scope(self.engine) as session:
            session.execute('delete from resumable_uploads where id = :resumable_id',
                            {'resumable_id': resumable_id})
            session.execute(f'drop table "{resumable_table}"')
        return True
