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

List of events that can be traced:

+----------------------------------------+-------------------------+------------------------------------+
| Logged event                           |  Event name             | msg_dict.keys()                    |
+========================================+=========================+====================================+
| Signalize Dispatcher run by Celery,    |                         | flow_name, dispatcher_id, args,    |
| each time a Dispatcher is              | `DISPATCHER_WAKEUP`     | retry, state, queue                |
| started/retried                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a flow start - called when   |                         |                                    |
| starting nodes are run                 | `FLOW_START`            | flow_name, dispatcher_id, args,    |
|                                        |                         | queue                              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a task scheduling by         | `TASK_SCHEDULE`         | flow_name, task_name, task_id,     |
| Dispatcher                             |                         | parent, args, queue                |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task start by Celery         | `TASK_START`            | flow_name, task_name, task_id,     |
|                                        |                         | parent, args                       |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize subflow start by Dispatcher  | `SUBFLOW_SCHEDULE`      | flow_name, dispatcher_id,          |
|                                        |                         | child_flow_name, args, parent      |
|                                        |                         | child_dispatcher_id, queue         |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize end of task from             | `TASK_END`              | flow_name, task_name, task_id,     |
| SelinonTaskEnvelope                    |                         | parent, args, storage              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a node success from          | `NODE_SUCCESSFUL`       | flow_name, dispatcher_id,          |
| Dispatcher                             |                         | node_name, node_id                 |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize that the result of task was  | `TASK_DISCARD_RESULT`   | flow_name, task_name, task_id,     |
| discarded                              |                         | parent, args, result               |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task failure from            | `TASK_FAILURE`          | flow_name, task_name, task_id,     |
| SelinonTaskEnvelope                    |                         | parent, args, what, retried_count  |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize task retry                   | `TASK_RETRY`            | flow_name, task_name, task_id,     |
|                                        |                         | parent, args, what, retried_count  |
|                                        |                         | max_retry, retry_countdown,        |
|                                        |                         | user_retry                         |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize when a flow ends because of  | `FLOW_FAILURE`          | flow_name, dispatcher_id, what     |
| error in nodes without fallback        |                         | queue                              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize unexpected dispatcher failure| `DISPATCHER_FAILURE`    | flow_name, dispatcher_id, what     |
| this should not occur (e.g. bug,       |                         | state, node_args, parent, finished |
| database connection error, ...)        |                         | retry, queue                       |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize a node failure from          | `NODE_FAILURE`          | flow_name, dispatcher_id,          |
| Dispatcher                             |                         | node_name, node_id, what           |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize fallback evaluation          | `FALLBACK_START`        | flow_name, dispatcher_id, nodes,   |
|                                        |                         | fallback                           |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize Dispatcher retry             | `DISPATCHER_RETRY`      | flow_name, dispatcher_id, retry,   |
|                                        |                         | state_dict, args, queue            |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalize flow end                     | `FLOW_END`              | flow_name, dispatcher_id,          |
|                                        |                         | finished_nodes, queue              |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage connect                 | `STORAGE_CONNECT`       | storage_name                       |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage disconnect              | `STORAGE_DISCONNECT`    | storage_name                       |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage access for reading      | `STORAGE_RETRIEVE`      | flow_name, task_name, storage      |
|                                        |                         |                                    |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signal storage access for writing      | `STORAGE_STORE`         | flow_name, task_name, task_id,     |
|                                        |                         | storage, result, record_id         |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalized when edge condition is      | `EDGE_COND_FALSE`       | nodes_to, nodes_from, flow_name,   |
| is evaluated as false                  |                         | parent, node_args, dispatcher_id   |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+
| Signalized result of foreach  ing      | `FOREACH_RESULT`        | nodes_to, nodes_from, flow_name,   |
|                                        |                         | parent, node_args, dispatcher_id   |
|                                        |                         |                                    |
+----------------------------------------+-------------------------+------------------------------------+

"""

import json
import platform
from celery.utils.log import get_task_logger


def _default_trace_func(event, msg_dict):
    # pylint: disable=unused-argument
    """
    Default tracing function that is used for storing results - do nothing

    :param event: event that triggered trace point
    :param msg_dict: a dict holding additional trace information for event
    """
    pass


class Trace(object):
    """
    Trace system flow actions
    """
    _trace_func = _default_trace_func
    _logger = None

    DISPATCHER_WAKEUP, \
    FLOW_START, \
    TASK_SCHEDULE, \
    TASK_START, \
    SUBFLOW_SCHEDULE, \
    TASK_END, \
    NODE_SUCCESSFUL, \
    TASK_DISCARD_RESULT, \
    TASK_FAILURE, \
    TASK_RETRY, \
    FLOW_FAILURE, \
    DISPATCHER_FAILURE, \
    NODE_FAILURE, \
    FALLBACK_START, \
    DISPATCHER_RETRY, \
    FLOW_END, \
    STORAGE_CONNECT, \
    STORAGE_DISCONNECT, \
    STORAGE_RETRIEVE, \
    STORAGE_RETRIEVED, \
    STORAGE_STORE, \
    STORAGE_STORED, \
    EDGE_COND_FALSE, \
    FOREACH_RESULT = range(24)

    _event_strings = [
        'DISPATCHER_WAKEUP',
        'FLOW_START',
        'TASK_SCHEDULE',
        'TASK_START',
        'SUBFLOW_SCHEDULE',
        'TASK_END',
        'NODE_SUCCESSFUL',
        'TASK_DISCARD_RESULT',
        'TASK_FAILURE',
        'TASK_RETRY',
        'FLOW_FAILURE',
        'DISPATCHER_FAILURE',
        'NODE_FAILURE',
        'FALLBACK_START',
        'DISPATCHER_RETRY',
        'FLOW_END',
        'STORAGE_CONNECT',
        'STORAGE_DISCONNECT',
        'STORAGE_RETRIEVE',
        'STORAGE_RETRIEVED',
        'STORAGE_STORE',
        'STORAGE_STORED',
        'EDGE_COND_FALSE',
        'FOREACH_RESULT'
    ]

    def __init__(self):
        raise NotImplementedError()

    @classmethod
    def trace_by_logging(cls, logger=None):
        """
        Trace by using Python's logging

        :param logger: optional logger that should be used
        """
        if not logger:
            logger = get_task_logger('selinon')

        cls._logger = logger
        cls._trace_func = cls.logging_trace_func

    @classmethod
    def trace_by_func(cls, func):
        """
        Trace by a custom function

        :param func: function with a one single argument
        """
        cls._trace_func = func

    @classmethod
    def log(cls, event, msg_dict):
        """
        Trace work

        :param event: tracing event
        :param msg_dict: message to be printed
        """
        cls._trace_func(event, msg_dict)

    @classmethod
    def event2str(cls, event):
        """
        Translate event to it's string representation

        :param event: event
        :return: string representation of event
        """
        return cls._event_strings[event]

    @classmethod
    def logging_trace_func(cls, event, msg_dict, logger=None):
        """
        Trace to Python's logging facilities

        :param event: event that triggered trace point
        :param msg_dict: a dict holding additional trace information for event
        :param logger: a logger to be used, if None, logger from trace_by_logging will be used
        """
        message = "SELINON %10s - %s: %s"\
                  % (platform.node(),
                     cls.event2str(event),
                     json.dumps(msg_dict, sort_keys=True))

        logger = logger or cls._logger
        if event in [Trace.NODE_FAILURE, Trace.DISPATCHER_FAILURE, Trace.TASK_DISCARD_RESULT, Trace.TASK_RETRY]:
            return logger.warn(message)
        else:
            return logger.info(message)
