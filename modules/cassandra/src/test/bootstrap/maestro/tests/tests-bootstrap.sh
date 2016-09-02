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
# Bootstrap script to spin up Tests cluster
# -----------------------------------------------------------------------------------------------

# URL to download JDK
JDK_DOWNLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/jdk-8u77-linux-x64.tar.gz

# URL to download Ignite-Cassandra tests package - you should previously package and upload it to this place
TESTS_PACKAGE_DONLOAD_URL=ecsb00100a91.epam.com:/opt/cassandra_summit_2016/ignite-cassandra-tests-1.8.0-SNAPSHOT.zip

# Terminates script execution
terminate()
{
    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Test node bootstrap failed"
        echo "[ERROR]-----------------------------------------------------"
        exit 1
    fi

    echo "[INFO]-----------------------------------------------------"
    echo "[INFO] Test node bootstrap successfully completed"
    echo "[INFO]-----------------------------------------------------"

    exit 0
}

# Downloads specified package
downloadPackage()
{
    echo "[INFO] Downloading $3 package from $1 into $2"

    for i in 0 9;
    do
        if [[ "$1" == http* ]] || [[ "$1" == https* ]] || [[ "$1" == ftp* ]] || [[ "$1" == ftps* ]]; then
            curl "$1" -o "$2"
            code=$?
        else
            username=$(echo $SSH_USER | sed -r "s/@/\\\@/g")
            src=$(echo $1 | sed -r "s/@/\\\@/g")
            dst=$(echo $2 | sed -r "s/@/\\\@/g")
            eval "sshpass -p \"$SSH_PASSWORD\" scp -rp -o \"StrictHostKeyChecking no\" ${username}@${src} $dst"
        fi

        if [ $code -eq 0 ]; then
            echo "[INFO] $3 package successfully downloaded from $1 into $2"
            return 0
        fi

        echo "[WARN] Failed to download $3 package from $i attempt, sleeping extra 5sec"
        sleep 5s
    done

    terminate "All 10 attempts to download $3 package from $1 are failed"
}

# Downloads and setup JDK
setupJava()
{
    rm -Rf /opt/java /opt/jdk.tar.gz

    echo "[INFO] Downloading 'jdk'"
    downloadPackage "$JDK_DOWNLOAD_URL" "/opt/jdk.tar.gz" "Java"
    if [ $? -ne 0 ]; then
        terminate "Failed to download 'jdk'"
    fi

    echo "[INFO] Untaring 'jdk'"
    tar -xvzf /opt/jdk.tar.gz -C /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to untar 'jdk'"
    fi

    rm -Rf /opt/jdk.tar.gz

    unzipDir=$(ls /opt | grep "jdk")
    if [ "$unzipDir" != "java" ]; then
        mv /opt/$unzipDir /opt/java
    fi
}

# Setup all the pre-requisites (packages, settings and etc.)
setupPreRequisites()
{
    echo "[INFO] Installing 'sshpass' package"
    yum -y install sshpass
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'sshpass' package"
    fi

    echo "[INFO] Installing 'htop', 'iptraf' and 'tcpdump' packages"
    yum -y install htop iptraf tcpdump
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to install 'htop', 'iptraf' and 'tcpdump' packages"
    fi

    echo "[INFO] Installing 'wget' package"
    yum -y install wget
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'wget' package"
    fi

    echo "[INFO] Installing 'net-tools' package"
    yum -y install net-tools
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'net-tools' package"
    fi

    echo "[INFO] Installing 'python' package"
    yum -y install python
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'python' package"
    fi

    echo "[INFO] Installing 'unzip' package"
    yum -y install unzip
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'unzip' package"
    fi

    downloadPackage "https://bootstrap.pypa.io/get-pip.py" "/opt/get-pip.py" "get-pip.py"

    echo "[INFO] Installing 'pip'"
    python /opt/get-pip.py
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'pip'"
    fi

    yum -y install htop iptraf tcpdump
}

# Downloads and setup tests package
setupTestsPackage()
{
    downloadPackage "$TESTS_PACKAGE_DONLOAD_URL" "/opt/ignite-cassandra-tests.zip" "Tests"

    rm -Rf /opt/ignite-cassandra-tests

    unzip /opt/ignite-cassandra-tests.zip -d /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to unzip tests package"
    fi

    rm -f /opt/ignite-cassandra-tests.zip

    unzipDir=$(ls /opt | grep "ignite-cassandra")
    if [ "$unzipDir" != "ignite-cassandra-tests" ]; then
        mv /opt/$unzipDir /opt/ignite-cassandra-tests
    fi

    find /opt/ignite-cassandra-tests -type f -name "*.sh" -exec chmod ug+x {} \;

    rm -Rf /opt/ignite-cassandra-tests/lib/ignite-cassandra-1.8.0-SNAPSHOT-tests-res.jar

    . /opt/ignite-cassandra-tests/bootstrap/maestro/common.sh "test"

    setupNTP

    bootstrapGangliaAgent "test" 8643

    ###################################################
    # Extra configuration specific only for test node #
    ###################################################

    echo "[INFO] Installing bc package"

    yum -y install bc

    if [ $? -ne 0 ]; then
        terminate "Failed to install bc package"
    fi

    echo "[INFO] Installing zip package"

    yum -y install zip

    if [ $? -ne 0 ]; then
        terminate "Failed to install zip package"
    fi

    echo "[INFO] Creating 'ignite' group"
    exists=$(cat /etc/group | grep ignite)
    if [ -z "$exists" ]; then
        groupadd ignite
        if [ $? -ne 0 ]; then
            terminate "Failed to create 'ignite' group"
        fi
    fi

    echo "[INFO] Creating 'ignite' user"
    exists=$(cat /etc/passwd | grep ignite)
    if [ -z "$exists" ]; then
        useradd -g ignite ignite
        if [ $? -ne 0 ]; then
            terminate "Failed to create 'ignite' user"
        fi
    fi

    mkdir -p /opt/ignite-cassandra-tests/logs
    chown -R ignite:ignite /opt/ignite-cassandra-tests

    echo "export JAVA_HOME=/opt/java" >> $1
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> $1
}

###################################################################################################################

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Bootstrapping Tests node"
echo "[INFO]-----------------------------------------------------------------"

export SSH_USER=$1
export SSH_PASSWORD=$2
export GANGLIA_MASTER=$3

setupPreRequisites
setupJava
setupTestsPackage "/root/.bash_profile"

terminate