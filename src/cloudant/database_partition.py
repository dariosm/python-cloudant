#!/usr/bin/env python
# Copyright (C) 2018 IBM Corp. All rights reserved.
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
"""
Database partition.
"""


class CouchDatabasePartition(object):
    """
    CouchDB database partition.

    :param partition_key: Partition key as string.
    """
    def __init__(self, db, partition_key):
        self._db = db
        self._partition_key = partition_key

    @property
    def partition_key(self):
        """
        Get partition key.

        :return: Partition key as string.
        """
        return self._partition_key

    def query(self, *args, **kwargs):
        """
        Run a query over this database partition.

        See :func:`~database.CouchDatabase.get_query_result`.

        :return: The result.
        """
        return self._db.get_query_result(*args,
                                         partition_key=self._partition_key,
                                         **kwargs)

    def view(self, *args, **kwargs):
        """
        Run a view over this database partition.

        See :func:`~database.CouchDatabase.get_view_result`.

        :return: The result.
        """
        return self._db.get_view_result(*args,
                                        partition_key=self._partition_key,
                                        **kwargs)


class CloudantDatabasePartition(CouchDatabasePartition):
    """
    Cloudant database partition.
    """
    def search(self, *args, **kwargs):
        """
        Run a search over this database partition.

        See :func:`~database.CloudantDatabase.get_search_result`.

        :return: The result.
        """
        return self._db.get_search_result(*args,
                                          partition_key=self._partition_key,
                                          **kwargs)
