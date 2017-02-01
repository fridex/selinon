#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""
Data storage interface
"""

import abc


class DataStorage(object, metaclass=abc.ABCMeta):
    """
    Abstract Selinon storage adapter that is implemented by a user
    """
    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def connect(self):
        """
        Connect to a resource, if not needed, should be empty
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self):
        """
        :return: True if connected to a resource
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def disconnect(self):
        """
        Disconnect from a resource
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def retrieve(self, flow_name, task_name, task_id):
        """
        Retrieve result stored in storage

        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be retrieved
        :param task_id: id of the task that result is going to be retrieved
        :return: task result
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store(self, node_args, flow_name, task_name, task_id, result):  # pylint: disable=too-many-arguments
        """
        Store result stored in storage

        :param node_args: arguments that were passed to node
        :param flow_name: flow name in which task was executed
        :param task_name: task name that result is going to be stored
        :param task_id: id of the task that result is going to be stored
        :param result: result that should be stored
        :return: unique ID of stored record
        """
        raise NotImplementedError()

    def __del__(self):
        if self.is_connected():
            self.disconnect()
