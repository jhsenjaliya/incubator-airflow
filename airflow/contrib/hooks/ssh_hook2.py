# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Ported to Airflow by Bolke de Bruin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is a port of Luigi's ssh implementation. All credits go there.

import logging
import os
import paramiko

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class SSHHook(BaseHook):
    """
    Hook for ssh remote execution using Paramiko.
    ref: https://github.com/paramiko/paramiko

    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer

    :param ssh_conn_id: connection id from airflow Connections
    :type ssh_conn_id: str
    :param remote_host: remote host to connect
    :type remote_host: str
    :param username: username to connect to the remote_host
    :type username: str
    :param password: password of the username to connect to the remote_host
    :type password: str
    :param key_file: key file to use to connect to the remote_host.
    :type key_file: str
    :param timeout: timeout for the attempt to connect to the remote_host.
    :type timeout: int
    :param compress: use compression while connection
    :type compress: bool
    """
    def __init__(self,
                 ssh_conn_id=None,
                 remote_host=None,
                 username=None,
                 password=None,
                 key_file=None,
                 timeout=10,
                 compress=True):
        super(SSHHook, self).__init__(ssh_conn_id)
        self.conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.key_file = key_file
        self.timeout = timeout
        self.compress = compress
        self.client = None

    def get_conn(self):
        if not self.client:
            logging.debug('creating ssh client for conn_id: %s', self.conn_id)
            if self.conn_id is not None:
                conn = self.get_connection(self.conn_id)
                if self.username is None:
                    self.username = conn.login
                if self.password is None:
                    self.password = conn.password
                if self.remote_host is None:
                    self.remote_host = conn.host
                if conn.extra is not None:
                    extra_options = conn.extra_dejson
                    self.key_file = extra_options.get("key_file")

                    if extra_options.has_key("timeout"):
                        self.timeout = int(extra_options.get("timeout", 10))

                    if extra_options.has_key("compress") \
                            and extra_options.get("compress").lower() == 'false':
                        self.compress = False

            if not self.username \
                    or not self.remote_host \
                    or not (self.password or self.key_file):
                raise AirflowException("Missing required parameters for SSHHook "
                                       "either username, password or key_file")

            try:
                client = paramiko.SSHClient()
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.WarningPolicy())

                ssh_conf = paramiko.SSHConfig()
                ssh_conf.parse(open(os.path.expanduser('~/.ssh/config')))
                host_info = ssh_conf.lookup(self.remote_host)
                host_proxy = None
                if host_info and host_info.get('proxycommand'):
                    host_proxy = paramiko.ProxyCommand(host_info.get('proxycommand'))

                if self.password and self.password.strip():
                    client.connect(hostname=self.remote_host,
                                   username=self.username,
                                   password=self.password,
                                   timeout=self.timeout,
                                   compress=self.compress,
                                   sock=host_proxy)
                else:
                    client.connect(hostname=self.remote_host,
                                   username=self.username,
                                   key_filename=self.key_file,
                                   timeout=self.timeout,
                                   compress=self.compress,
                                   sock=host_proxy)

                self.client = client
            except paramiko.AuthenticationException as auth_error:
                logging.error("Auth failed while connecting to host: %s, error: %s",
                              self.remote_host, auth_error)
            except paramiko.SSHException as ssh_error:
                logging.error("Failed connecting to host: %s, error: %s",
                              self.remote_host, ssh_error)
            except Exception as error:
                logging.error("Error connecting to host: %s, error: %s",
                              self.remote_host, error)
        return self.client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()

