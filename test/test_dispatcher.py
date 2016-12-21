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

import pytest
from flexmock import flexmock
from selinonTestCase import SelinonTestCase
from selinon import Dispatcher
from selinon.systemState import SystemState
from selinon import FlowError
from requestMock import RequestMock


class TestDispatcher(SelinonTestCase):
    def test_run(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'failed_nodes': {}, 'finished_nodes': {'Task1': ['<task-id>']}}

        flexmock(SystemState).should_receive('update').and_return(None)
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()

        assert dispatcher.run(flow_name) == state_dict

    def test_flow_error(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'failed_nodes': {'Task1': ['<task1-id>']}, 'finished_nodes': {}}

        flexmock(SystemState).should_receive('update').and_raise(FlowError)
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').and_return(FlowError)

        # We should improve this by actually checking exception value
        with pytest.raises(FlowError):
            dispatcher.run(flow_name)

    def test_dispacher_error(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        exc = KeyError("some exception in dispatcher")
        flexmock(SystemState).should_receive('update').and_raise(exc)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').and_return(exc)

        # We should improve this by actually checking exception value
        with pytest.raises(KeyError):
            dispatcher.run(flow_name)

    def test_retry(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'finished_nodes': {'Task1': ['<task1-id>']}, 'failed_nodes': {}}

        flexmock(SystemState).should_receive('update').and_raise(2)  # we will retry
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').and_return(FlowError)

        # We should improve this by actually checking exception value
        with pytest.raises(FlowError):
            dispatcher.run(flow_name)
