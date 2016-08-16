#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

from .lockPool import LockPool


class StoragePool(object):
    """
    A pool that carries all database connections for workers
    """
    _storage_mapping = {}

    def __init__(self, id_mapping=None):
        self._id_mapping = id_mapping if id_mapping else {}

    @classmethod
    def set_storage_mapping(cls, storage_mapping):
        """
        :param storage_mapping: database adapters that should be used for certain task in a flow flow
        """
        cls._storage_mapping = storage_mapping

    @classmethod
    def _connected_storage(cls, storage_name):
        # if this raises KeyError exception it means that the flow was not configured properly - should
        # be handled by Parsley
        storage = cls._storage_mapping[storage_name]

        if not storage.connected():
            with LockPool.get_lock(storage):
                if not storage.connected():
                    # TODO: we could optimize this by limiting number of active connections
                    storage.connect()

        return storage

    def get(self, storage_name, flow_name, task_name):
        storage = self._connected_storage(storage_name)
        return storage.retrieve(flow_name, task_name, self._id_mapping[task_name])

    @classmethod
    def set(cls, storage_name, flow_name, task_name, task_id, result):
        storage = cls._connected_storage(storage_name)
        storage.store(flow_name, task_name, task_id, result)