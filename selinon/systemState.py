#!/bin/env python3
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
Main system actions done by Selinon
"""

import itertools
import json
import datetime
import copy
from threading import Lock
from functools import reduce
from collections import deque
from celery.result import AsyncResult
from .errors import FlowError
from .storagePool import StoragePool
from .trace import Trace
from .config import Config
from .selinonTaskEnvelope import SelinonTaskEnvelope


class SystemState(object):  # pylint: disable=too-many-instance-attributes
    """
    Main system actions done by Selinon
    """
    # Throttled nodes in the current worker: node name -> next schedule time
    # Throttle should be safe with concurrency
    _throttle_lock = Lock()
    _throttled_tasks = {}
    _throttled_flows = {}

    @property
    def node_args(self):
        """
        :return: arguments for a node
        """
        return self._node_args

    @staticmethod
    def _get_async_result(id):  # pylint: disable=invalid-name,redefined-builtin
        return AsyncResult(id=id)

    @classmethod
    def _instantiate_active_nodes(cls, arr):
        """
        :return: convert node references from argument to AsyncResult
        """
        return [{'name': node['name'], 'id': node['id'],
                 'result': cls._get_async_result(node['id'])} for node in arr]

    @staticmethod
    def _deinstantiate_active_nodes(arr):
        """
        :return: node references for Dispatcher arguments
        """
        return [{'name': x['name'], 'id': x['id']} for x in arr]

    @staticmethod
    def _idxs2items(edge_node_table, arr):
        # we keep only the index to edges_table in order to avoid serialization and optimize number representation
        ret = []
        for i in arr:
            ret.append(edge_node_table[i])
        return ret

    def __repr__(self):
        # Make tests more readable
        return str(self.to_dict())

    def __init__(self, dispatcher_id, flow_name, node_args=None, retry=None, state=None, parent=None):
        # pylint: disable=too-many-arguments
        state_dict = state or {}

        self._dispatcher_id = dispatcher_id
        self._flow_name = flow_name
        self._node_args = node_args
        self._parent = parent or {}
        self._active_nodes = self._instantiate_active_nodes(state_dict.get('active_nodes', []))
        self._finished_nodes = state_dict.get('finished_nodes', {})
        self._failed_nodes = state_dict.get('failed_nodes', {})
        self._waiting_edges_idx = state_dict.get('waiting_edges', [])
        # Instantiate lazily later if we will know that there is something to process
        self._waiting_edges = []
        self._retry = retry

    def to_dict(self):
        """
        :return: converted system state to dict
        """
        return {
            'active_nodes': self._deinstantiate_active_nodes(self._active_nodes),
            'finished_nodes': self._finished_nodes,
            'failed_nodes': self._failed_nodes,
            'waiting_edges': self._waiting_edges_idx
        }

    def _get_countdown(self, node_name, is_flow):
        """
        Get countdown for throttling

        :param node_name: node name
        :param is_flow: true if node_name is a flow
        :return: countdown seconds for the current schedule
        """
        if is_flow:
            throttle_conf = Config.throttle_flows
            throttled_nodes = self._throttled_flows
        else:
            throttle_conf = Config.throttle_tasks
            throttled_nodes = self._throttled_tasks

        with self._throttle_lock:
            if throttle_conf[node_name]:
                current_datetime = datetime.datetime.now()
                if node_name not in throttled_nodes:
                    # we throttle for the first time
                    throttled_nodes[node_name] = current_datetime
                    return None

                next_run = current_datetime + throttle_conf[node_name]
                countdown = (throttled_nodes[node_name] + throttle_conf[node_name] - current_datetime).total_seconds()
                throttled_nodes[node_name] = next_run

                return countdown if countdown > 0 else None
            else:
                return None

    def _get_successful_and_failed(self):
        """
        :return: all successful and failed nodes in system from active nodes
        """
        ret_successful = []
        ret_failed = []

        new_active_nodes = []
        for node in self._active_nodes:
            if node['result'].successful():
                Trace.log(Trace.NODE_SUCCESSFUL, {'flow_name': self._flow_name,
                                                  'dispatcher_id': self._dispatcher_id,
                                                  'node_name': node['name'],
                                                  'node_id': node['id']})
                ret_successful.append(node)
            elif node['result'].failed():
                Trace.log(Trace.NODE_FAILURE, {'flow_name': self._flow_name,
                                               'dispatcher_id': self._dispatcher_id,
                                               'node_name': node['name'],
                                               'node_id': node['id'],
                                               'what': node['result'].result})
                # We keep track of failed nodes to handle failures once all nodes finish
                if node['name'] not in self._failed_nodes:
                    self._failed_nodes[node['name']] = []
                self._failed_nodes[node['name']].append(node['id'])
                ret_failed.append(node['id'])
            else:
                new_active_nodes.append(node)

        self._active_nodes = new_active_nodes

        return ret_successful, ret_failed

    def _start_node(self, node_name, parent, node_args, force_propagate_node_args=False):
        """
        Start a node in the system

        :param node_name: name of a node to be started
        :param parent: parent nodes of the starting node
        :param node_args: arguments for the starting node
        """
        from .dispatcher import Dispatcher
        if Config.is_flow(node_name):
            if force_propagate_node_args or Config.should_propagate_node_args(self._flow_name, node_name):
                node_args = node_args
            else:
                node_args = None

            if Config.should_propagate_parent(self._flow_name, node_name):
                parent = parent
            else:
                parent = None

            kwargs = {
                'flow_name': node_name,
                'node_args': node_args,
                'parent': parent,
            }

            countdown = self._get_countdown(node_name, is_flow=True)
            async_result = Dispatcher().apply_async(kwargs=kwargs,
                                                    queue=Config.dispatcher_queues[node_name],
                                                    countdown=countdown)

            # reuse kwargs for trace log entry
            kwargs['meta'] = {
                'flow_name': self._flow_name,
                'child_flow_name': node_name,
                'dispatcher_id': self._dispatcher_id,
                'child_dispatcher_id': async_result.task_id,
                'queue': Config.dispatcher_queues[node_name],
                'countdown': countdown
            }

            Trace.log(Trace.SUBFLOW_SCHEDULE, kwargs)
        else:
            kwargs = {
                'task_name': node_name,
                'flow_name': self._flow_name,
                'parent': parent,
                'node_args': node_args,
                'dispatcher_id': self._dispatcher_id
            }

            countdown = self._get_countdown(node_name, is_flow=False)
            async_result = SelinonTaskEnvelope().apply_async(kwargs=kwargs,
                                                             queue=Config.task_queues[node_name],
                                                             countdown=countdown)

            # reuse kwargs for trace log entry
            kwargs['meta'] = {
                'task_id': async_result.task_id,
                'queue': Config.task_queues[node_name],
                'countdown': countdown
            }

            Trace.log(Trace.TASK_SCHEDULE, kwargs)

        record = {'name': node_name, 'id': async_result.task_id, 'result': async_result}

        if node_name not in Config.nowait_nodes.get(self._flow_name, []):
            self._active_nodes.append(record)

        return record

    def _fire_edge(self, edge, storage_pool, parent, node_args):
        """
        Fire edge - start new nodes as described in edge table

        :param edge: edge that should be fired
        :param storage_pool: storage pool which makes results of previous tasks available
        :param parent: parent nodes
        :param node_args: node arguments
        :return: list of nodes that were scheduled
        """
        ret = []

        if 'foreach' in edge:
            iterable = edge['foreach'](storage_pool, node_args)
            Trace.log(Trace.FOREACH_RESULT, {
                'nodes_to': edge['to'],
                'nodes_from': edge['from'],
                'flow_name': self._flow_name,
                'foreach_str': edge['foreach_str'],
                'parent': parent,
                'node_args': self._node_args,
                'dispatcher_id': self._dispatcher_id
            })
            # handle None as well
            if not iterable:
                return ret
            for res in iterable:
                for node_name in edge['to']:
                    if edge.get('foreach_propagate_result'):
                        record = self._start_node(node_name, parent, res, force_propagate_node_args=True)
                    else:
                        record = self._start_node(node_name, parent, node_args)
                    ret.append(record)
        else:
            for node_name in edge['to']:
                record = self._start_node(node_name, parent, node_args)
                ret.append(record)

        return ret

    def _run_fallback(self):
        """
        Run fallback in the system

        :return: fallbacks that were run
        """
        # we sort it first to make evaluation dependent on alphabetical order
        # TODO: use binary search when inserting to optimize from O(N*log(N)) to O(log(N))
        ret = []
        failed_nodes = sorted(self._failed_nodes.items())

        # pylint: disable=too-many-nested-blocks
        for i in range(len(failed_nodes), 0, -1):
            for combination in itertools.combinations(failed_nodes, i):
                change = True
                while change:
                    change = False
                    try:
                        failure_nodes = Config.failures[self._flow_name]
                        failure_node = reduce(lambda n, c: n['next'][c[0]], combination[1:],
                                              failure_nodes[combination[0][0]])
                    except KeyError:
                        # such failure not found in the tree of permutations - this means that this
                        # flow will always fail, but run defined fallbacks for defined failures first
                        continue

                    if isinstance(failure_node['fallback'], list) and len(failure_node['fallback']) > 0:
                        traced_nodes_arr = []

                        for node in combination:
                            traced_nodes_arr.append({'name': node[0], 'id': self._failed_nodes[node[0]][0]})
                            self._failed_nodes[node[0]].pop(0)
                            if len(self._failed_nodes[node[0]]) == 0:
                                del self._failed_nodes[node[0]]

                        Trace.log(Trace.FALLBACK_START, {'flow_name': self._flow_name,
                                                         'dispatcher_id': self._dispatcher_id,
                                                         'nodes': traced_nodes_arr,
                                                         'fallback': failure_node['fallback']})

                        for node in failure_node['fallback']:
                            # TODO: parent should be tasks from traced_nodes_arr, we need to compute sub-flow ids
                            record = self._start_node(node, parent=None, node_args=self._node_args)
                            ret.append(record)

                        # wait for fallback to finish in order to avoid time dependent flow evaluation
                        return ret
                    elif failure_node['fallback'] is True:
                        change = True
                        traced_nodes_arr = []
                        for node in combination:
                            traced_nodes_arr.append({'name': node[0], 'id': self._failed_nodes[node[0]][0]})
                            self._failed_nodes[node[0]].pop(0)
                            if len(self._failed_nodes[node[0]]) == 0:
                                del self._failed_nodes[node[0]]
                                # we have reached zero in failed nodes, we cannot continue with failure
                                # combination otherwise we get KeyError
                                change = False

                        Trace.log(Trace.FALLBACK_START, {'flow_name': self._flow_name,
                                                         'dispatcher_id': self._dispatcher_id,
                                                         'nodes': traced_nodes_arr,
                                                         'fallback': True})
                        # continue with fallback in other combinations, nothing started

        return ret

    def _update_waiting_edges(self, node_name):
        """
        Update waiting edges (edges that wait for a node finish) based on node name

        :param node_name: node that will trigger an edge
        """
        for idx, edge in enumerate(Config.edge_table[self._flow_name]):
            if node_name in edge['from'] and idx not in self._waiting_edges_idx:
                self._waiting_edges.append(edge)
                self._waiting_edges_idx.append(idx)

    @classmethod
    def _extend_parent_from_flow(cls, dst_dict, flow_name, flow_id, key, compound=False):
        # pylint: disable=too-many-arguments,too-many-locals
        """
        Compute parent in a flow in case of propagate_parent flag

        :param dst_dict: a dictionary that should be extended with calculated parents
        :param flow_name: a flow name for which propagate_parent is calculated
        :param flow_id: flow identifier as identified in Celery
        :param key: key under which should be result stored
        :param compound: true if propagate_compound was used
        """
        def follow_keys(d, k):
            # pylint: disable=invalid-name,missing-docstring
            if not compound:
                # A key is always a flow name except the last key, which is a task name with list of task ids
                for m_k in k:
                    if m_k not in d:
                        d[m_k] = {}
                    d = d[m_k]
                return d
            else:
                # If we have compound, the key to list of task is flow name.
                # This will allow to use conditions as expected.
                # e.g.:
                #  {'flow1': {'Task1': ['task1_id1', 'task1_id2'],
                #            {'Task2': ['task2_id1', 'task2_id2']}
                if flow_name not in d:
                    d[flow_name] = {}
                return d[flow_name]

        def push_flow(s, p_n_name, flow_result, k):
            # pylint: disable=invalid-name,missing-docstring
            for n_name, n_ids in flow_result[key].items():
                if not compound:
                    shallow_k = copy.copy(k)
                    shallow_k.append(p_n_name)
                    s.append((n_name, n_ids, shallow_k))
                else:
                    s.append((n_name, n_ids, k))

        res = dst_dict

        async_result = cls._get_async_result(flow_id)
        stack = deque()
        push_flow(stack, flow_name, async_result.result, deque())

        while stack:
            node_name, node_ids, keys = stack.pop()
            if Config.is_flow(node_name):
                for node_id in node_ids:
                    push_flow(stack, node_name, cls._get_async_result(node_id).result, keys)
            else:
                dst = follow_keys(res, keys)

                if node_name not in dst:
                    dst[node_name] = []

                for node_id in node_ids:
                    dst[node_name].append(node_id)

        return res

    def _start_new_from_finished(self, new_finished):  # pylint: disable=too-many-locals
        """
        Start new based on finished nodes

        :param new_finished: finished nodes based on which we should start new nodes
        :return: newly started nodes
        """
        # pylint: disable=too-many-nested-blocks,too-many-branches
        ret = []

        if len(new_finished) == 1 and len(self._active_nodes) == 0 and len(self._finished_nodes) == 0:
            # propagate arguments from newly finished node if configured to do so
            if Config.node_args_from_first.get(self._flow_name, False):
                self._node_args = StoragePool.retrieve(new_finished[0]['name'], new_finished[0]['id'])

        for node in new_finished:
            # We could optimize this by pre-computing affected edges in pre-generated config file for each
            # node and computing intersection with waiting edges, but let's stick with this solution for now
            edges = [edge for edge in self._waiting_edges if node['name'] in edge['from']]

            for edge in edges:
                from_nodes = dict.fromkeys(edge['from'], [])

                for from_name in from_nodes:
                    if from_name == node['name']:
                        from_nodes[from_name] = [{'name': node['name'], 'id': node['id']}]
                    else:
                        from_nodes[from_name] = [{'name': from_name, 'id': n}
                                                 for n in self._finished_nodes.get(from_name, [])]

                # if there are multiple nodes of a same type waiting on an edge:
                #
                #   A    B
                #    \  /
                #     \/
                #     C
                #
                # A: <id1>, <id2>
                # B: <id3>, <id4>
                #
                # Compute all combinations: (<id1>, <id3>), (<id1>, <id4>), (<id2>, <id3>), (<id2>, <id4>)
                #
                for start_nodes in itertools.product(*from_nodes.values()):
                    parent = {}
                    storage_id_mapping = {}

                    for start_node in start_nodes:
                        if Config.is_flow(start_node['name']):
                            compound = Config.should_propagate_compound_finished(self._flow_name,
                                                                                 start_node['name'])
                            if Config.should_propagate_finished(self._flow_name, start_node['name']) or compound:
                                parent = self._extend_parent_from_flow(parent, start_node['name'], start_node['id'],
                                                                       'finished_nodes', compound)
                        else:
                            parent[start_node['name']] = start_node['id']
                            storage_id_mapping[start_node['name']] = start_node['id']

                    # We could also examine results of subflow, there could be passed a list of subflows with
                    # finished_nodes to 'condition' in order to do inspection
                    storage_pool = StoragePool(storage_id_mapping)
                    if edge['condition'](storage_pool, self._node_args):
                        records = self._fire_edge(edge, storage_pool, parent=parent, node_args=self._node_args)
                        ret.extend(records)
                    else:
                        Trace.log(Trace.EDGE_COND_FALSE, {
                            'nodes_to': edge['to'],
                            'nodes_from': edge['from'],
                            'condition': edge['condition_str'],
                            'flow_name': self._flow_name,
                            'parent': parent,
                            'node_args': self._node_args,
                            'dispatcher_id': self._dispatcher_id
                        })

            node_name = node['name']
            if not self._finished_nodes.get(node_name):
                self._finished_nodes[node_name] = []
            self._finished_nodes[node_name].append(node['id'])

        return ret

    def _start_and_update_retry(self):
        """
        Start the flow and update retry

        :return: new/next retry
        """
        new_started_nodes = []

        Trace.log(Trace.FLOW_START, {'flow_name': self._flow_name,
                                     'dispatcher_id': self._dispatcher_id,
                                     'queue': Config.dispatcher_queues[self._flow_name],
                                     'args': self._node_args})
        start_edges = [edge for edge in Config.edge_table[self._flow_name] if len(edge['from']) == 0]
        if len(start_edges) == 0:
            # This should not occur since selinonlib raises exception if such occurs, but just to be sure...
            raise ValueError("No starting node found for flow '%s'!" % self._flow_name)

        for start_edge in start_edges:
            storage_pool = StoragePool(self._parent)
            if start_edge['condition'](storage_pool, self._node_args):
                records = self._fire_edge(start_edge, storage_pool, node_args=self._node_args, parent=self._parent)

                for node in records:
                    if node['name'] not in Config.nowait_nodes.get(self._flow_name, []):
                        self._update_waiting_edges(node['name'])
                        new_started_nodes.append(node)

        self._retry = Config.strategies[self._flow_name](previous_retry=None,
                                                         active_nodes=self._active_nodes,
                                                         failed_nodes=self._failed_nodes,
                                                         new_started_nodes=new_started_nodes,
                                                         new_fallback_nodes=[],
                                                         finished_nodes=self._finished_nodes)

    def update(self):
        """
        Check the current state in the system and start new nodes if possible

        :return: retry count - can be None (do not retry dispatcher) or time in seconds to retry
        """

        if not self._active_nodes and not self._finished_nodes and not self._waiting_edges and not self._failed_nodes:
            # we are starting up
            self._start_and_update_retry()
        else:
            new_finished, new_failed = self._get_successful_and_failed()

            if new_finished or new_failed:
                # Instantiate results lazily
                self._waiting_edges = self._idxs2items(Config.edge_table[self._flow_name], self._waiting_edges_idx)

                for node in new_finished:
                    self._update_waiting_edges(node['name'])

                started = self._start_new_from_finished(new_finished)

                fallback_started = []
                # We wait until all active nodes finish if there are some failed nodes try to recover from failure,
                # otherwise mark flow as failed
                if len(self._active_nodes) == 0 and self._failed_nodes:
                    fallback_started = self._run_fallback()
                    if len(fallback_started) == 0 and len(self._failed_nodes) > 0:
                        # We will propagate state information so we can correctly propagate parent and failed nodes in
                        # parent flow if there is any
                        # We use JSON in order to use result backend with JSON configured so objects are not pickled
                        state_dict = self.to_dict()
                        state_info = {
                            'finished_nodes': state_dict['finished_nodes'],
                            'failed_nodes': state_dict['failed_nodes']
                        }
                        raise FlowError(json.dumps(state_info))

                self._retry = Config.strategies[self._flow_name](previous_retry=self._retry,
                                                                 active_nodes=self._active_nodes,
                                                                 failed_nodes=self._failed_nodes,
                                                                 new_started_nodes=started,
                                                                 new_fallback_nodes=fallback_started,
                                                                 finished_nodes=self._finished_nodes)
            else:
                self._retry = Config.strategies[self._flow_name](previous_retry=self._retry,
                                                                 active_nodes=self._active_nodes,
                                                                 failed_nodes=self._failed_nodes,
                                                                 new_started_nodes=[],
                                                                 new_fallback_nodes=[],
                                                                 finished_nodes=self._finished_nodes)
        return self._retry
