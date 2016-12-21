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
Errors that can be used or can occur outside Selinon
"""


class FatalTaskError(Exception):
    """
    An exception that is raised by task on fatal error - task will be not retried
    """
    pass


class InternalError(Exception):
    """
    Internal error of Selinon project, should not occur for end-user
    """
    pass


class ConfigError(Exception):
    """
    Error raised when there is an error when parsing configuration files
    """
    pass

class FlowError(Exception):
    """
    An exception that is raised once there is an error in the flow
    """
    pass
