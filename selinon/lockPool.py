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
Global lock pool for shared locks
"""

from threading import Lock


class LockPool(object):  # pylint: disable=too-few-public-methods
    """
    Global lock pool for shared locks
    """
    _locks = {}
    _global_lock = Lock()

    @classmethod
    def get_lock(cls, lock_id):
        """
        Get lock for resource, exclusively

        :param lock_id: lock_id for uniquely identify lock
        :return: lock, can be acquired if already taken, if new, always released
        """
        # TODO: we could make it more optimal by storing only locks that are used and release them once unused
        with cls._global_lock:
            if not cls._locks.get(lock_id):
                ret = Lock()
                cls._locks[lock_id] = ret
            else:
                ret = cls._locks[lock_id]

        return ret
