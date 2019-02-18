# Copyright 2019 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import urllib.parse
import leveldb

from loopchain.store.key_value_store import KeyValueStoreError
from loopchain.store.key_value_store import KeyValueStoreWriteBatch, KeyValueStoreCancelableWriteBatch, KeyValueStore
from loopchain.store.key_value_store import _validate_args_bytes_without_first


def _error_convert(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except leveldb.LevelDBError as e:
            raise KeyValueStoreError(e)

    return _wrapper


class _KeyValueStoreWriteBatchLevelDb(KeyValueStoreWriteBatch):
    def __init__(self, db: leveldb.LevelDB, sync: bool):
        self.__db = db
        self.__batch = self.__new_batch()
        self.__sync = sync

    @_error_convert
    def __new_batch(self):
        return leveldb.WriteBatch()

    @_validate_args_bytes_without_first
    @_error_convert
    def put(self, key: bytes, value: bytes):
        self.__batch.Put(key, value)

    @_validate_args_bytes_without_first
    @_error_convert
    def delete(self, key: bytes):
        self.__batch.Delete(key)

    @_error_convert
    def clear(self):
        del self.__batch
        self.__batch = self.__new_batch()

    @_error_convert
    def write(self):
        self.__db.Write(self.__batch, sync=self.__sync)


class _KeyValueStoreCancelableWriteBatchLevelDb(KeyValueStoreCancelableWriteBatch):
    def __init__(self, store: KeyValueStore, db: leveldb.LevelDB, sync: bool):
        super().__init__(store, sync=sync)
        self.__touched_keys = set()
        self.__snapshot = db.CreateSnapshot()

    def _touch(self, key: bytes):
        self.__touched_keys.add(key)

    def _get_original_touched_item(self):
        for key in self.__touched_keys:
            try:
                yield key, self.__snapshot.get(key)
            except KeyError:
                return key, None

    def clear(self):
        super().clear()
        self.__touched_keys.clear()

    def close(self):
        del self.__snapshot
        self.__snapshot = None


class KeyValueStoreLevelDb(KeyValueStore):
    TYPE = 'leveldb'

    def __init__(self, uri: str, **kwargs):
        uri_obj = urllib.parse.urlparse(uri)
        if uri_obj.scheme != 'file':
            raise ValueError(f"Support file path URI only (ex. file:///xxx/xxx). uri={uri}")
        self.__db = self.__new_db(uri_obj.path, **kwargs)

    @_error_convert
    def __new_db(self, path, **kwargs) -> leveldb.LevelDB:
        return leveldb.LevelDB(path, **kwargs)

    @_validate_args_bytes_without_first
    @_error_convert
    def get(self, key, default=None, **kwargs):
        try:
            return self.__db.Get(key, **kwargs)
        except KeyError:
            if default is not None:
                return default
            raise KeyError(f"Has no value of key({key})")

    @_validate_args_bytes_without_first
    @_error_convert
    def put(self, key, value, sync=False, **kwargs):
        self.__db.Put(key, value, sync=sync, **kwargs)

    @_validate_args_bytes_without_first
    @_error_convert
    def delete(self, key, sync=False, **kwargs):
        self.__db.Delete(key, sync=sync, **kwargs)

    @_error_convert
    def close(self):
        del self.__db
        self.__db = None

    @_error_convert
    def WriteBatch(self, sync=False) -> KeyValueStoreWriteBatch:
        return _KeyValueStoreWriteBatchLevelDb(self.__db, sync)

    @_error_convert
    def CancelableWriteBatch(self, sync=False) -> KeyValueStoreCancelableWriteBatch:
        return _KeyValueStoreCancelableWriteBatchLevelDb(self, self.__db, sync)
