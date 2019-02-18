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


import abc
import functools


class KeyValueStoreError(Exception):
    pass


class KeyValueStoreWriteBatch(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def put(self, key: bytes, value: bytes):
        raise NotImplementedError("put() function is interface method")

    @abc.abstractmethod
    def delete(self, key: bytes):
        raise NotImplementedError("delete() function is interface method")

    @abc.abstractmethod
    def clear(self):
        raise NotImplementedError("clear() function is interface method")

    @abc.abstractmethod
    def write(self):
        raise NotImplementedError("write() function is interface method")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.write()


class KeyValueStoreCancelableWriteBatch(metaclass=abc.ABCMeta):
    def __init__(self, store: 'KeyValueStore', sync=False):
        self.__store = store
        self.__batch = self.__store.WriteBatch(sync=sync)
        self.__sync = sync

    def put(self, key: bytes, value: bytes):
        self.__batch.put(key, value)
        self._touch(key)

    def delete(self, key: bytes):
        self.__batch.delete(key)
        self._touch(key)

    def clear(self):
        self.__batch.clear()

    def write(self):
        self.__batch.write()

    def cancel(self):
        batch = self.__store.WriteBatch(sync=self.__sync)
        for key, value in self._get_original_touched_item():
            if value is None:
                batch.delete(key)
            else:
                batch.put(key, value)
        batch.write()

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError("close() function is interface method")

    @abc.abstractmethod
    def _touch(self, key: bytes):
        raise NotImplementedError("_touch() function is interface method")

    @abc.abstractmethod
    def _get_original_touched_item(self):
        # Children have to override function as generator. return key, value
        raise NotImplementedError("_get_touched_item() function is interface method")


class KeyValueStore(metaclass=abc.ABCMeta):
    TYPE = None

    @abc.abstractmethod
    def get(self, key: bytes, default=None, **kwargs):
        raise NotImplementedError("get() function is interface method")

    @abc.abstractmethod
    def put(self, key: bytes, value: bytes, sync=False, **kwargs):
        raise NotImplementedError("put() function is interface method")

    @abc.abstractmethod
    def delete(self, key: bytes, sync=False, **kwargs):
        raise NotImplementedError("delete() function is interface method")

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError("close() function is interface method")

    @abc.abstractmethod
    def WriteBatch(self, sync=False) -> KeyValueStoreWriteBatch:
        raise NotImplementedError("WriteBatch constructor is not implemented in KeyValueStore class")

    @abc.abstractmethod
    def CancelableWriteBatch(self, sync=False) -> KeyValueStoreCancelableWriteBatch:
        raise NotImplementedError("CancelableWriteBatch constructor is not implemented in KeyValueStore class")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def _validate_args_bytes_without_first(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        for arg in args[1:]:
            if not isinstance(arg, bytes):
                raise ValueError(f"Argument type({type(arg)}) is not bytes. argument={arg}")
        return func(*args, **kwargs)

    return _wrapper
