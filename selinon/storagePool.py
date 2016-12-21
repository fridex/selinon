#!/usr/bin/env python3
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
"""
A pool that carries all database connections for workers
"""

from .config import Config
from .lockPool import LockPool
from .trace import Trace


class StoragePool(object):
    """
    A pool that carries all database connections for workers
    """
    def __init__(self, id_mapping=None):
        self._id_mapping = id_mapping or {}

    @classmethod
    def get_storage_name_by_task_name(cls, task_name, graceful=False):
        """
        :param task_name: name of a task
        :param graceful: return None instead of raising an exception
        :return: storage name for task
        """
        storage = Config.task2storage_mapping.get(task_name)

        if storage is None and not graceful:
            raise KeyError("No storage for task '%s' defined" % task_name)

        return storage

    @classmethod
    def get_storage_by_task_name(cls, task_name):
        """
        :param task_name: task's name for which storage should be get
        :rtype: DataStorage
        """
        storage_name = cls.get_storage_name_by_task_name(task_name, graceful=True)
        if storage_name:
            return cls.get_connected_storage(storage_name)
        else:
            return None

    @classmethod
    def get_connected_storage(cls, storage_name):
        """
        Retrieve connected storage based by its name stated in configuration

        :param storage_name: name of storage
        :return: connected storage
        """
        # if this raises KeyError exception it means that the flow was not configured properly - should
        # be handled by Selinonlib
        storage = Config.storage_mapping[storage_name]

        if not storage.is_connected():
            with LockPool.get_lock(storage):
                if not storage.is_connected():
                    Trace.log(Trace.STORAGE_CONNECT, {'storage_name': storage_name})
                    storage.connect()

        return storage

    def get(self, task_name):
        """
        Retrieve data for task based on mapping for the current context

        :param task_name: task's name that we are retrieving data for
        :return: task's result for the current context
        """
        return self.retrieve(task_name, self._id_mapping[task_name])

    @classmethod
    def retrieve(cls, task_name, task_id):
        """
        Retrieve task's result from database which was configured to be used for desired task

        :param task_name: name of task for which result should be retrieved
        :param task_id: task ID to uniquely identify task results
        :return: task's result
        """
        storage = cls.get_storage_by_task_name(task_name)
        storage_task_name = Config.storage_task_name[task_name]
        Trace.log(Trace.STORAGE_RETRIEVE, {'task_name': task_name,
                                           'storage_task_name': storage_task_name,
                                           'storage_name': Config.task2storage_mapping[task_name]})
        return storage.retrieve(task_name, task_id)

    @classmethod
    def set(cls, node_args, flow_name, task_name, task_id, result):
        # pylint: disable=too-many-arguments
        """
        Store result for task

        :param node_args: arguments that were passed to the node
        :param flow_name: flow in which task was run
        :param task_name: task that computed result
        :param task_id: task id that computed result
        :param result: result that should be stored
        :return: result ID - a unique ID which can be used to reference task results
        """
        storage = cls.get_storage_by_task_name(task_name)
        storage_task_name = Config.storage_task_name[task_name]

        record_id = storage.store(node_args, flow_name, storage_task_name, task_id, result)
        Trace.log(Trace.STORAGE_STORE, {'flow_name': flow_name,
                                        'node_args': node_args,
                                        'task_name': task_name,
                                        'storage_task_name': storage_task_name,
                                        'task_id': task_id,
                                        'storage_name': Config.task2storage_mapping[task_name],
                                        'result': result,
                                        'record_id': record_id})
        return record_id
