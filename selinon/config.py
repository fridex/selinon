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
All user configurations generated from YAML file
"""

import os
import runpy
import tempfile
import logging
from selinonlib import System
from .trace import Trace


class Config(object):
    """
    All user configurations generated from YAML file
    """
    _logger = logging.getLogger(__name__)

    celery_app = None

    flows = None
    task_classes = None
    edge_table = None
    nowait_nodes = None
    max_retry = None
    retry_countdown = None
    storage2storage_cache = None
    storage_readonly = None
    storage_task_name = None
    propagate_node_args = None
    propagate_parent = None
    propagate_finished = None
    propagate_compound_finished = None
    output_schemas = None

    storage_mapping = None
    task2storage_mapping = None
    dispatcher_queues = None
    task_queues = None
    strategies = None

    @classmethod
    def _set_config(cls, config_module):
        """
        Set configuration from Python's module
        """
        cls.task_classes = config_module['task_classes']
        cls.edge_table = config_module['edge_table']
        cls.failures = config_module['failures']
        cls.nowait_nodes = config_module['nowait_nodes']
        cls.flows = list(cls.edge_table.keys())

        # misc
        cls.node_args_from_first = config_module['node_args_from_first']

        # propagate_* entries
        cls.propagate_finished = config_module['propagate_finished']
        cls.propagate_node_args = config_module['propagate_node_args']
        cls.propagate_parent = config_module['propagate_parent']
        cls.propagate_compound_finished = config_module['propagate_compound_finished']

        # task configuration
        cls.output_schemas = config_module['output_schemas']
        cls.storage_mapping = config_module['storage2instance_mapping']
        cls.storage_task_name = config_module['storage_task_name']
        cls.task2storage_mapping = config_module['task2storage_mapping']
        cls.max_retry = config_module['max_retry']
        cls.retry_countdown = config_module['retry_countdown']
        cls.storage_readonly = config_module['storage_readonly']
        cls.storage2storage_cache = config_module['storage2storage_cache']

        # throttle configuration
        cls.throttle_tasks = config_module['throttle_tasks']
        cls.throttle_flows = config_module['throttle_flows']

        # queues
        cls.dispatcher_queues = config_module['dispatcher_queues']
        cls.task_queues = config_module['task_queues']

        # Dispatcher scheduling strategy
        cls.strategies = config_module['strategies']

        # call config init with Config class to set up other configuration specific values
        config_module['init'](cls)

    @classmethod
    def set_config_py(cls, config_code):
        """
        Set dispatcher configuration by Python config file

        :param config_code: configuration source code
        """
        cls._logger.debug("Using config.py file from '%s'", config_code)
        config_module = runpy.run_path(config_code)
        cls._set_config(config_module)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files, config_py=None, keep_config_py=False):
        """
        Set dispatcher configuration by path to YAML configuration files

        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        :param config_py: a file that should be used for storing generated config.py
        :param keep_config_py: do not remove config_py file after run
        """
        system = System.from_files(nodes_definition_file, flow_definition_files)

        if not config_py:
            tmp_f = tempfile.NamedTemporaryFile(mode="w", delete=False)
            cls._logger.debug("Generating config.py file to created temporary file '%s'", tmp_f.name)
            system.dump2stream(tmp_f)
            tmp_f.close()
            config_py = tmp_f.name
        else:
            cls._logger.debug("Generating config.py file to proposed file '%s'", config_py)
            with open(config_py, "w") as output_f:
                system.dump2stream(output_f)

        cls.set_config_py(config_py)

        if not keep_config_py:
            cls._logger.debug("Removing generated config.py file '%s'", config_py)
            os.unlink(config_py)

    @classmethod
    def trace_by_func(cls, trace_func):
        """
        Set tracing function for Dispatcher

        :param trace_func: a function that should be used to trace dispatcher actions
        """
        Trace.trace_by_func(trace_func)

    @classmethod
    def trace_by_logging(cls):
        """
        Use Python's logging for tracing
        """
        Trace.trace_by_logging()

    @classmethod
    def set_celery_app(cls, celery_app):
        """
        Set celery application that should be used

        :param celery_app: celery app instance
        """
        # Avoid circular imports
        from .dispatcher import Dispatcher
        from .selinonTaskEnvelope import SelinonTaskEnvelope

        cls._logger.debug("Registering Selinon to Celery context")

        cls.celery_app = celery_app
        celery_app.tasks.register(Dispatcher())
        celery_app.tasks.register(SelinonTaskEnvelope())

    @staticmethod
    def _should_config(node_name, dst_node_name, configuration):
        """
        :param node_name: node name
        :param dst_node_name: destination node to which configuration should be propagated
        :param configuration: configuration that should be checked
        :return: true if node_name satisfies configuration
        """
        if configuration[node_name] is True:
            return True

        if isinstance(configuration[node_name], list):
            return dst_node_name in configuration[node_name]

        return False

    @classmethod
    def should_propagate_finished(cls, node_name, dst_node_name):
        """
        :param node_name: node name that should be checked for propagate_finished
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_finish
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_finished)

    @classmethod
    def should_propagate_node_args(cls, node_name, dst_node_name):
        """
        :param node_name: node name that should be checked for propagate_node_args
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_node_args
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_node_args)

    @classmethod
    def should_propagate_parent(cls, node_name, dst_node_name):
        """
        :param node_name: node name that should be checked for propagate_parent
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_parent
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_parent)

    @classmethod
    def should_propagate_compound_finished(cls, node_name, dst_node_name):  # pylint: disable=invalid-name
        """
        :param node_name: node name that should be checked for propagate_compound_finished
        :param dst_node_name: destination node to which configuration should be propagated
        :return: True if should propagate_compound_finished
        """
        return cls._should_config(node_name, dst_node_name, cls.propagate_compound_finished)

    @classmethod
    def get_task_instance(cls, task_name, flow_name, parent, dispatcher_id):
        """
        Get instance of SelinonTask

        :param task_name: task name that should be instantiated (it is not necessarily SelinonTask)
        :param flow_name: flow name
        :param parent: parent nodes for instance
        :param dispatcher_id: id of dispatcher for flow flow
        :return: instance of the task
        """
        task_class = Config.task_classes[task_name]
        return task_class(task_name=task_name, flow_name=flow_name, parent=parent, dispatcher_id=dispatcher_id)

    @classmethod
    def is_flow(cls, node_name):
        """
        Check if given node is a flow by its name

        :param node_name: name of the node to be checked
        :return: True if given node is a flow
        """
        return node_name in cls.flows
