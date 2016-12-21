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
Base class for user-defined tasks
"""

import abc
from celery.result import AsyncResult
from .storagePool import StoragePool
from .retry import Retry


class SelinonTask(metaclass=abc.ABCMeta):
    """
    Base class for user-defined tasks
    """
    def __init__(self, flow_name, task_name, parent, dispatcher_id):
        """
        :param flow_name: name of flow under which this tasks runs on
        :param task_name: name of task, note it can be aliased since we can have different task name and class name
        :param parent: direct task's predecessors stated in flow dependency
        :parent dispatcher_id: id of dispatcher handling the current flow
        """
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent
        self.dispatcher_id = dispatcher_id

    def _selinon_dereference_task_id(self, flow_names, task_name, index):
        """
        Compute task id based on mapping of ancestors (from parent sub-flows)

        :param flow_names: name of parent flow or list of flow names in case of nested flows
        :param task_name: name of task in parent flow
        :param index: index of result if more than one subflow was run
        :return: task id based from parent subflows
        """
        if not isinstance(flow_names, list):
            flow_names = [flow_names]

        parent_flow = self.parent
        for flow_name in flow_names:
            try:
                parent_flow = parent_flow[flow_name]
            except KeyError:
                raise KeyError("No such parent flow '%s' for task '%s', check your configuration; nested "
                               "as %s from flow %s"
                               % (flow_name, self.task_name, flow_names, self.flow_name))
        try:
            task_id = parent_flow[task_name][index]
        except KeyError:
            raise KeyError("No such parent task '%s' dereferenced by '%s' was run for task '%s' in flow '%s'"
                           % (task_name, flow_names, self.task_name, self.flow_name))
        except IndexError:
            raise IndexError("Requested index %s in parent task '%s' dereferencered by '%s', but there were run only "
                             "%s tasks in task %s flow %s"
                             % (index, task_name, flow_names, len(parent_flow[task_name]), self.task_name,
                                self.flow_name))
        return task_id

    @property
    def storage(self):
        """
        :return: tasks's configured storage as stated in YAML config
        """
        return StoragePool.get_storage_by_task_name(self.task_name)

    def parent_task_result(self, parent_name):
        """
        :param parent_name: name of parent task to retrieve result from
        :return: result of parent task
        """
        try:
            parent_task_id = self.parent[parent_name]
        except KeyError:
            raise KeyError("No such parent '%s' in task '%s' in flow '%s', check your configuration"
                           % (parent_name, self.task_name, self.flow_name))

        return StoragePool.retrieve(parent_name, parent_task_id)

    def parent_flow_result(self, flow_names, task_name, index=None):
        """
        Get parent subflow results; note that parent flows can return multiple results from task of same type
        because of loops in flows

        :param flow_names: name of parent flow or list of flow names in case of nested flows
        :param task_name: name of task in parent flow
        :param index: index of result if more than one subflow was run
        :return: result of task in parent subflow
        """
        index = -1 if index is None else index
        task_id = self._selinon_dereference_task_id(flow_names, task_name, index)
        return StoragePool.retrieve(task_name, task_id)

    def parent_task_exception(self, parent_name):
        """Retrieve parent task exception. You have to call this from a fallback (direct or transitive)

        :param task_name: name of task that failed (ancestor of calling task)
        :return exception that was raised in the ancestor
        """
        try:
            parent_task_id = self.parent[parent_name]
        except KeyError:
            raise KeyError("No such parent '%s' in task '%s' in flow '%s', check your configuration"
                           % (parent_name, self.task_name, self.flow_name))

        # Celery stores exceptions in
        celery_result = AsyncResult(parent_task_id)
        if not celery_result.failed():
            raise ValueError("Parent task '%s' did not raised exception" % parent_name)

        return celery_result.result

    def parent_flow_exception(self, flow_names, task_name, index=None):
        """Retrieve parent task exception. You have to call this from a fallback (direct or transitive)

        :param task_name: name of task that failed (ancestor of calling task)
        :return exception that was raised in the ancestor
        """
        index = -1 if index is None else index
        task_id = self._selinon_dereference_task_id(flow_names, task_name, index)

        # Celery stores exceptions in result field
        celery_result = AsyncResult(task_id)
        if not celery_result.failed():
            raise ValueError("Parent task '%s' from subflows %s did not raised exception"
                             % (task_name, str(flow_names)))

        return celery_result.result

    @staticmethod
    def retry(countdown=0):
        """
        Retry, always raises Retry, this is compatible with Celery's self.retry() except you cannot modify arguments

        :param countdown: countdown for rescheduling
        """
        raise Retry(countdown)

    @abc.abstractmethod
    def run(self, node_args):
        """
        Task's entrypoint - user defined computation

        :param node_args: arguments passed to flow/node
        :return: tasks's result that will be stored in database as configured
        """
        pass
