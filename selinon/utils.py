#!/bin/bin/env python3
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
Library utilities for a user
"""

from .config import Config
from .dispatcher import Dispatcher


def run_flow(flow_name, node_args=None):
    """
    Run flow by it's name

    :param flow_name: name of the flow to be run
    :param node_args: arguments that will be supplied to flow
    :return: flow ID (dispatcher ID)
    """
    if Config.dispatcher_queues is None or flow_name not in Config.dispatcher_queues:
        raise KeyError("No flow with name '%s' defined" % flow_name)

    return Dispatcher().apply_async(kwargs={'flow_name': flow_name,
                                            'node_args': node_args},
                                    queue=Config.dispatcher_queues[flow_name])
