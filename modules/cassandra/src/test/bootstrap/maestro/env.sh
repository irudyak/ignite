#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -----------------------------------------------------------------------------------------------
# This file specifies environment specific settings to bootstrap required infrastructure for:
# -----------------------------------------------------------------------------------------------
#
#   1) Cassandra cluster
#   2) Ignite cluster
#   3) Tests cluster
#   4) Ganglia agents to be installed on each clusters machine
#   5) Ganglia master to collect metrics from agent and show graphs on Ganglia Web dashboard
#
# -----------------------------------------------------------------------------------------------

# Time (in minutes) to wait for Cassandra/Ignite node up and running and register it in S3
export SERVICE_STARTUP_TIME=10

# Number of attempts to start Cassandra/Ignite daemon
export SERVICE_START_ATTEMPTS=3

# Cassandra related settings
export CASSANDRA_DOWNLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/apache-cassandra-3.5-bin.tar.gz

# Ignite related settings
export IGNITE_DOWNLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/apache-ignite-fabric-1.8.0-SNAPSHOT-bin.zip

# Ganglia related settings
export GANGLIA_CORE_DOWNLOAD_URL=https://github.com/ganglia/monitor-core.git
export GANGLIA_WEB_DOWNLOAD_URL=https://github.com/ganglia/ganglia-web.git
export RRD_DOWNLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/rrdtool-1.3.1.tar.gz
export GPERF_DOWNLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/gperf-3.0.3.tar.gz
