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
A raw Celery task that is responsible for running SelinonTask
"""

import traceback
import json
import jsonschema
from celery import Task
from .storagePool import StoragePool
from .errors import FatalTaskError
from .trace import Trace
from .config import Config
from .retry import Retry


class SelinonTaskEnvelope(Task):
    """
    A Celery task that is responsible for running user defined tasks from flow
    """
    # Celery configuration
    ignore_result = False
    acks_late = True
    track_started = True
    max_retries = None
    name = "selinon.SelinonTaskEnvelope"

    @classmethod
    def validate_result(cls, task_name, result):
        """
        Validate result of the task for the given schema, if fails an Exception is raised

        :param task_name: name of task
        :param result: result of task
        """
        schema_path = Config.output_schemas.get(task_name)
        if schema_path:
            with open(schema_path, "r") as input_file:
                schema = json.load(input_file)

            jsonschema.validate(result, schema)

    def selinon_retry(self, task_name, flow_name, parent, node_args, retry_countdown, retried_count,
                      dispatcher_id, user_retry=False):
        # pylint: disable=too-many-arguments
        """Retry on Celery level

        :param task_name: name of the task to be retried
        :param flow_name: name of in which the task was run
        :param parent: dict of parent tasks
        :param node_args: arguments for task
        :param retry_countdown: countdown for retry
        :param retried_count: number of retries already done with this task
        :param dispatcher_id: ID id of dispatcher that is handling flow that run this task
        :param user_retry: True if retry was forced from the user
        """
        max_retry = Config.max_retry.get(task_name, 0)
        kwargs = {
            'task_name': task_name,
            'flow_name': flow_name,
            'parent': parent,
            'node_args': node_args,
            'dispatcher_id': dispatcher_id,
            'retried_count': retried_count
        }

        Trace.log(Trace.TASK_RETRY, {'flow_name': flow_name,
                                     'task_name': task_name,
                                     'task_id': self.request.id,
                                     'parent': parent,
                                     'args': node_args,
                                     'what': traceback.format_exc(),
                                     'retry_countdown': retry_countdown,
                                     'retried_count': retried_count,
                                     'user_retry': user_retry,
                                     'queue': Config.task_queues[task_name],
                                     'dispatcher_id': dispatcher_id,
                                     'max_retry': max_retry})
        raise self.retry(kwargs=kwargs, countdown=retry_countdown, queue=Config.task_queues[task_name])

    def run(self, task_name, flow_name, parent, node_args, dispatcher_id, retried_count=None):
        # pylint: disable=arguments-differ,too-many-arguments
        """
        Task entry-point called by Celery

        :param task_name: task to be run
        :param flow_name: flow in which this task run
        :param parent: dict of parent nodes
        :param node_args: node arguments within the flow
        :param dispatcher_id: dispatcher id that handles flow
        :param retried_count: number of already attempts that failed so task was retried
        :rtype: None
        """
        # we are passing args as one argument explicitly for now not to have troubles with *args and **kwargs mapping
        # since we depend on previous task and the result can be anything
        Trace.log(Trace.TASK_START, {'flow_name': flow_name,
                                     'task_name': task_name,
                                     'task_id': self.request.id,
                                     'parent': parent,
                                     'queue': Config.task_queues[task_name],
                                     'dispatcher_id': dispatcher_id,
                                     'args': node_args})
        try:
            task = Config.get_task_instance(task_name=task_name, flow_name=flow_name, parent=parent,
                                            dispatcher_id=dispatcher_id)
            result = task.run(node_args)
            self.validate_result(task_name, result)

            storage = StoragePool.get_storage_by_task_name(task_name)
            if storage and not Config.storage_readonly[task_name]:
                storage.store(node_args, flow_name, task_name, self.request.id, result)
            elif result is not None:
                Trace.log(Trace.TASK_DISCARD_RESULT, {'flow_name': flow_name,
                                                      'task_name': task_name,
                                                      'task_id': self.request.id,
                                                      'parent': parent,
                                                      'args': node_args,
                                                      'queue': Config.task_queues[task_name],
                                                      'dispatcher_id': dispatcher_id,
                                                      'result': result})
        except Retry as retry:
            # we do not touch retried_count
            self.selinon_retry(task_name, flow_name, parent, node_args, retry.countdown, retried_count,
                               dispatcher_id, user_retry=True)
        except Exception as exc:  # pylint: disable=broad-except
            max_retry = Config.max_retry.get(task_name, 0)
            retried_count = retried_count or 0

            if max_retry > retried_count and not isinstance(exc, FatalTaskError):
                retried_count += 1
                retry_countdown = Config.retry_countdown.get(task_name, 0)
                self.selinon_retry(task_name, flow_name, parent, node_args, retry_countdown, retried_count,
                                   dispatcher_id)
            else:
                Trace.log(Trace.TASK_FAILURE, {'flow_name': flow_name,
                                               'task_name': task_name,
                                               'task_id': self.request.id,
                                               'parent': parent,
                                               'args': node_args,
                                               'what': traceback.format_exc(),
                                               'queue': Config.task_queues[task_name],
                                               'dispatcher_id': dispatcher_id,
                                               'retried_count': retried_count})
                raise self.retry(max_retries=0, exc=exc)

        Trace.log(Trace.TASK_END, {'flow_name': flow_name,
                                   'task_name': task_name,
                                   'task_id': self.request.id,
                                   'parent': parent,
                                   'args': node_args,
                                   'queue': Config.task_queues[task_name],
                                   'dispatcher_id': dispatcher_id,
                                   'storage': StoragePool.get_storage_name_by_task_name(task_name, graceful=True)})
