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
import plyvel

from loopchain.store.key_value_store import KeyValueStoreError
from loopchain.store.key_value_store import KeyValueStoreWriteBatch, KeyValueStoreCancelableWriteBatch, KeyValueStore
from loopchain.store.key_value_store import _validate_args_bytes_without_first


def _error_convert(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except plyvel.Error as e:
            raise KeyValueStoreError(e)

    return _wrapper


class _KeyValueStoreWriteBatchPlyvel(KeyValueStoreWriteBatch):
    def __init__(self, db: plyvel.DB, sync: bool):
        self.__batch = self.__new_batch(db, sync)

    @_error_convert
    def __new_batch(self, db: plyvel.DB, sync: bool):
        return db.write_batch(sync=sync)

    @_validate_args_bytes_without_first
    @_error_convert
    def put(self, key: bytes, value: bytes):
        self.__batch.put(key, value)

    @_validate_args_bytes_without_first
    @_error_convert
    def delete(self, key: bytes):
        self.__batch.delete(key)

    @_error_convert
    def clear(self):
        self.__batch.clear()

    @_error_convert
    def write(self):
        self.__batch.write()


class _KeyValueStoreCancelableWriteBatchPlyvel(KeyValueStoreCancelableWriteBatch):
    def __init__(self, store: KeyValueStore, db: plyvel.DB, sync: bool):
        super().__init__(store, sync=sync)
        self.__touched_keys = set()
        self.__snapshot = db.snapshot()

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
        self.__snapshot.close()
        self.__snapshot = None


class KeyValueStorePlyvel(KeyValueStore):
    TYPE = 'plyvel'

    def __init__(self, uri: str, **kwargs):
        uri_obj = urllib.parse.urlparse(uri)
        if uri_obj.scheme != 'file':
            raise ValueError(f"Support file path URI only (ex. file:///xxx/xxx). uri={uri}")
        self.__db = self.__new_db(uri_obj.path, **kwargs)

    @_error_convert
    def __new_db(self, path, **kwargs) -> plyvel.DB:
        return plyvel.DB(path, **kwargs)

    @_validate_args_bytes_without_first
    @_error_convert
    def get(self, key: bytes, default=None, **kwargs):
        result = self.__db.get(key, default=default, **kwargs)
        if result is None:
            raise KeyError(f"Has no value of key({key})")
        return result

    @_validate_args_bytes_without_first
    @_error_convert
    def put(self, key: bytes, value: bytes, sync=False, **kwargs):
        self.__db.put(key, value, sync=sync, **kwargs)

    @_validate_args_bytes_without_first
    @_error_convert
    def delete(self, key: bytes, sync=False, **kwargs):
        self.__db.delete(key, sync=sync, **kwargs)

    @_error_convert
    def close(self):
        self.__db.close()

    @_error_convert
    def WriteBatch(self, sync=False) -> KeyValueStoreWriteBatch:
        return _KeyValueStoreWriteBatchPlyvel(self.__db, sync=sync)

    @_error_convert
    def CancelableWriteBatch(self, sync=False) -> KeyValueStoreCancelableWriteBatch:
        return _KeyValueStoreCancelableWriteBatchPlyvel(self, self.__db, sync=sync)
