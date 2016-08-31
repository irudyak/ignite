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
# Common purpose functions used by bootstrap scripts
# -----------------------------------------------------------------------------------------------

# Clone git repository
gitClone()
{
    echo "[INFO] Cloning git repository $1 to $2"

    rm -Rf $2

    for i in 0 9;
    do
        git clone $1 $2

        if [ $code -eq 0 ]; then
            echo "[INFO] Git repository $1 was successfully cloned to $2"
            return 0
        fi

        echo "[WARN] Failed to clone git repository $1 from $i attempt, sleeping extra 5sec"
        rm -Rf $2
        sleep 5s
    done

    terminate "All 10 attempts to clone git repository $1 are failed"
}

# Sets NODE_TYPE env variable
setNodeType()
{
    if [ -n "$1" ]; then
        NEW_NODE_TYPE=$NODE_TYPE
        NODE_TYPE=$1
    else
        NEW_NODE_TYPE=
    fi
}

# Reverts NODE_TYPE env variable to previous value
revertNodeType()
{
    if [ -n "$NEW_NODE_TYPE" ]; then
        NODE_TYPE=$NEW_NODE_TYPE
        NEW_NODE_TYPE=
    fi
}

# Terminates script execution
terminate()
{
    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Failed to start $NODE_TYPE node"
        echo "[ERROR]-----------------------------------------------------"
        exit 1
    fi

    echo "[INFO]-----------------------------------------------------"
    echo "[INFO] $NODE_TYPE node successfully started"
    echo "[INFO]-----------------------------------------------------"
}

# Installs all required Ganglia packages
installGangliaPackages()
{
    if [ "$1" == "master" ]; then
        echo "[INFO] Installing Ganglia master required packages"
    else
        echo "[INFO] Installing Ganglia agent required packages"
    fi

    isAmazonLinux=$(cat "/etc/issue" | grep "Amazon Linux")

    if [ -z "$isAmazonLinux" ]; then
        isDisabled=$(sestatus -v | grep "disabled")

        if [ -z "$isDisabled" ]; then
            setenforce 0

            if [ $? -ne 0 ]; then
                terminate "Failed to turn off SELinux"
            fi
        fi
    fi

    yum -y install apr-devel apr-util check-devel cairo-devel pango-devel pango \
    libxml2-devel glib2-devel dbus-devel freetype-devel freetype \
    libpng-devel libart_lgpl-devel fontconfig-devel gcc-c++ expat-devel \
    python-devel libXrender-devel perl-devel perl-CPAN gettext git sysstat \
    automake autoconf ltmain.sh pkg-config gperf libtool pcre-devel libconfuse-devel

    if [ $? -ne 0 ]; then
        terminate "Failed to install all Ganglia required packages"
    fi

    if [ "$1" == "master" ]; then
        yum -y install httpd php php-devel php-pear

        if [ $? -ne 0 ]; then
            terminate "Failed to install all Ganglia required packages"
        fi

        if [ -z "$isAmazonLinux" ]; then
            yum -y install liberation-sans-fonts

            if [ $? -ne 0 ]; then
                terminate "Failed to install liberation-sans-fonts package"
            fi
        fi
    fi

    if [ -z "$isAmazonLinux" ]; then
        downloadPackage "$GPERF_DOWNLOAD_URL" "/opt/gperf.tar.gz" "gperf"

        tar -xvzf /opt/gperf.tar.gz -C /opt
        if [ $? -ne 0 ]; then
            terminate "Failed to untar gperf tarball"
        fi

        rm -Rf /opt/gperf.tar.gz

        unzipDir=$(ls /opt | grep "gperf")

        if [ $? -ne 0 ]; then
            terminate "Failed to update creation date to current for all files inside: /opt/$unzipDir"
        fi

        pushd /opt/$unzipDir

        cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
        rm ./configure
        mv ./configure1 ./configure
        chmod a+x ./configure

        ./configure
        if [ $? -ne 0 ]; then
            terminate "Failed to configure gperf"
        fi

        make
        if [ $? -ne 0 ]; then
            terminate "Failed to make gperf"
        fi

        make install
        if [ $? -ne 0 ]; then
            terminate "Failed to install gperf"
        fi

        echo "[INFO] gperf tool successfully installed"

        popd
    fi

    echo "[INFO] Installing rrdtool"

    downloadPackage "$RRD_DOWNLOAD_URL" "/opt/rrdtool.tar.gz" "rrdtool"

    tar -xvzf /opt/rrdtool.tar.gz -C /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to untar rrdtool tarball"
    fi

    rm -Rf /opt/rrdtool.tar.gz

    unzipDir=$(ls /opt | grep "rrdtool")
    if [ "$unzipDir" != "rrdtool" ]; then
        mv /opt/$unzipDir /opt/rrdtool
    fi

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/rrdtool"
    fi

    export PKG_CONFIG_PATH=/usr/lib/pkgconfig/

    pushd /opt/rrdtool

    cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
    rm ./configure
    mv ./configure1 ./configure
    chmod a+x ./configure

    yum -y remove ruby

    ./configure --prefix=/usr/local/rrdtool
    if [ $? -ne 0 ]; then
        terminate "Failed to configure rrdtool"
    fi

    yum -y remove ruby

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make rrdtool"
    fi

    yum -y remove ruby

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install rrdtool"
    fi

    ln -s /usr/local/rrdtool/bin/rrdtool /usr/bin/rrdtool
    mkdir -p /var/lib/ganglia/rrds

    chown -R nobody:nobody /usr/local/rrdtool /var/lib/ganglia/rrds /usr/bin/rrdtool

    rm -Rf /opt/rrdtool

    popd

    echo "[INFO] rrdtool successfully installed"

    echo "[INFO] Installig ganglia-core"

    gitClone $GANGLIA_CORE_DOWNLOAD_URL /opt/monitor-core

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/monitor-core"
    fi

    pushd /opt/monitor-core

    git checkout efe9b5e5712ea74c04e3b15a06eb21900e18db40

    yum -y remove ruby

    ./bootstrap

    if [ $? -ne 0 ]; then
        terminate "Failed to prepare ganglia-core for compilation"
    fi

    cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
    rm ./configure
    mv ./configure1 ./configure
    chmod a+x ./configure

    yum -y remove ruby

    ./configure --with-gmetad --with-librrd=/usr/local/rrdtool

    if [ $? -ne 0 ]; then
        terminate "Failed to configure ganglia-core"
    fi

    yum -y remove ruby

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make ganglia-core"
    fi

    yum -y remove ruby

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install ganglia-core"
    fi

    rm -Rf /opt/monitor-core

    popd

    echo "[INFO] ganglia-core successfully installed"

    if [ "$1" != "master" ]; then
        return 0
    fi

    echo "[INFO] Installing ganglia-web"

    gitClone $GANGLIA_WEB_DOWNLOAD_URL /opt/web

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/web"
    fi

    cat /opt/web/Makefile | sed -r "s/GDESTDIR = \/usr\/share\/ganglia-webfrontend/GDESTDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile1
    cat /opt/web/Makefile1 | sed -r "s/GCONFDIR = \/etc\/ganglia-web/GCONFDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile2
    cat /opt/web/Makefile2 | sed -r "s/GWEB_STATEDIR = \/var\/lib\/ganglia-web/GWEB_STATEDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile3
    cat /opt/web/Makefile3 | sed -r "s/APACHE_USER = www-data/APACHE_USER = apache/g" > /opt/web/Makefile4

    rm -f /opt/web/Makefile
    cp /opt/web/Makefile4 /opt/web/Makefile
    rm -f /opt/web/Makefile1 /opt/web/Makefile2 /opt/web/Makefile3 /opt/web/Makefile4

    pushd /opt/web

    git checkout f2b19c7cacfc8c51921be801b92f8ed0bd4901ae

    yum -y remove ruby

    make

    if [ $? -ne 0 ]; then
        terminate "Failed to make ganglia-web"
    fi

    yum -y remove ruby

    make install

    if [ $? -ne 0 ]; then
        terminate "Failed to install ganglia-web"
    fi

    rm -Rf /opt/web

    popd

    echo "" >> /etc/httpd/conf/httpd.conf
    echo "Alias /ganglia /opt/ganglia-web" >> /etc/httpd/conf/httpd.conf
    echo "<Directory \"/opt/ganglia-web\">" >> /etc/httpd/conf/httpd.conf
    echo "       AllowOverride All" >> /etc/httpd/conf/httpd.conf
    echo "       Order allow,deny" >> /etc/httpd/conf/httpd.conf

    if [ -z "$isAmazonLinux" ]; then
        echo "       Require all granted" >> /etc/httpd/conf/httpd.conf
    fi

    echo "       Allow from all" >> /etc/httpd/conf/httpd.conf
    echo "       Deny from none" >> /etc/httpd/conf/httpd.conf
    echo "</Directory>" >> /etc/httpd/conf/httpd.conf

    echo "[INFO] ganglia-web successfully installed"
}

# Setup ntpd service
setupNTP()
{
    echo "[INFO] Installing ntp package"

    yum -y install ntp

    if [ $? -ne 0 ]; then
        terminate "Failed to install ntp package"
    fi

    echo "[INFO] Starting ntpd service"

    service ntpd restart

    if [ $? -ne 0 ]; then
        terminate "Failed to restart ntpd service"
    fi
}

# Installs and run Ganglia agent ('gmond' daemon)
bootstrapGangliaAgent()
{
    echo "[INFO]-----------------------------------------------------------------"
    echo "[INFO] Bootstrapping Ganglia agent"
    echo "[INFO]-----------------------------------------------------------------"

    installGangliaPackages

    echo "[INFO] Running ganglia agent daemon to discover Ganglia master"

    /opt/ignite-cassandra-tests/bootstrap/maestro/ganglia/agent-start.sh $1 $2 > /opt/ganglia-agent.log &

    echo "[INFO] Ganglia daemon job id: $!"
}

# Partitioning, formatting to ext4 and mounting all unpartitioned drives.
# As a result env array MOUNT_POINTS provides all newly created mount points.
mountUnpartitionedDrives()
{
    MOUNT_POINTS=

    echo "[INFO] Mounting unpartitioned drives"

    lsblk -V &> /dev/null

    if [ $? -ne 0 ]; then
        echo "[WARN] lsblk utility doesn't exist"
        echo "[INFO] Installing util-linux-ng package"

        yum -y install util-linux-ng

        if [ $? -ne 0 ]; then
            terminate "Failed to install util-linux-ng package"
        fi
    fi

    parted -v &> /dev/null

    if [ $? -ne 0 ]; then
        echo "[WARN] parted utility doesn't exist"
        echo "[INFO] Installing parted package"

        yum -y install parted

        if [ $? -ne 0 ]; then
            terminate "Failed to install parted package"
        fi
    fi

    drives=$(lsblk -io KNAME,TYPE | grep disk | sed -r "s/disk//g" | xargs)

    echo "[INFO] Found HDDs: $drives"

    unpartDrives=
    partDrives=$(lsblk -io KNAME,TYPE | grep part | sed -r "s/[0-9]*//g" | sed -r "s/part//g" | xargs)

    drives=($drives)
	count=${#drives[@]}
	iter=1

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		drive=${drives[$i]}

        if [ -z "$drive" ]; then
            continue
        fi

        isPartitioned=$(echo $partDrives | grep "$drive")

        if [ -n "$isPartitioned" ] || [ "$drive" == "fd0" ]; then
            continue
        fi

        echo "[INFO] Creating partition for the drive: $drive"

        parted -s -a opt /dev/$drive mklabel gpt mkpart primary 0% 100%

        if [ $? -ne 0 ]; then
            terminate "Failed to create partition for the drive: $drive"
        fi

        partition=$(lsblk -io KNAME,TYPE | grep part | grep $drive | sed -r "s/part//g" | xargs)

        if [ -z "$partition" ]; then
            sleep 5s
            partition=$(lsblk -io KNAME,TYPE | grep part | grep $drive | sed -r "s/part//g" | xargs)
        fi

        echo "[INFO] Successfully created partition $partition for the drive: $drive"

        if [ -z "$partition" ]; then
            echo "[WARNING] Failed to detect created partition name"
            partition="sdb1"
        fi

        echo "[INFO] Formatting partition /dev/$partition to ext4"

        mkfs.ext4 -F -q /dev/$partition

        if [ $? -ne 0 ]; then
            terminate "Failed to format partition: /dev/$partition"
        fi

        echo "[INFO] Partition /dev/$partition was successfully formatted to ext4"

        echo "[INFO] Mounting partition /dev/$partition to /storage$iter"

        mkdir -p /storage$iter

        if [ $? -ne 0 ]; then
            terminate "Failed to create mount point directory: /storage$iter"
        fi

        echo "/dev/$partition               /storage$iter               ext4    defaults        1 1" >> /etc/fstab

        mount /storage$iter

        if [ $? -ne 0 ]; then
            terminate "Failed to mount /storage$iter mount point for partition /dev/$partition"
        fi

        echo "[INFO] Partition /dev/$partition was successfully mounted to /storage$iter"

        if [ -n "$MOUNT_POINTS" ]; then
            MOUNT_POINTS="$MOUNT_POINTS "
        fi

        MOUNT_POINTS="${MOUNT_POINTS}/storage${iter}"

        iter=$(($iter+1))
    done

    if [ -z "$MOUNT_POINTS" ]; then
        echo "[INFO] All drives already have partitions created"
    fi

    MOUNT_POINTS=($MOUNT_POINTS)
}

# Creates storage directories for Cassandra: data files, commit log, saved caches.
# As a result CASSANDRA_DATA_DIR, CASSANDRA_COMMITLOG_DIR, CASSANDRA_CACHES_DIR will point to appropriate directories.
createCassandraStorageLayout()
{
    CASSANDRA_DATA_DIR=
    CASSANDRA_COMMITLOG_DIR=
    CASSANDRA_CACHES_DIR=

    mountUnpartitionedDrives

    echo "[INFO] Creating Cassandra storage layout"

	count=${#MOUNT_POINTS[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        mountPoint=${MOUNT_POINTS[$i]}

        if [ -z "$CASSANDRA_DATA_DIR" ]; then
            CASSANDRA_DATA_DIR=$mountPoint
        elif [ -z "$CASSANDRA_COMMITLOG_DIR" ]; then
            CASSANDRA_COMMITLOG_DIR=$mountPoint
        elif [ -z "$CASSANDRA_CACHES_DIR" ]; then
            CASSANDRA_CACHES_DIR=$mountPoint
        else
            CASSANDRA_DATA_DIR="$CASSANDRA_DATA_DIR $mountPoint"
        fi
    done

    if [ -z "$CASSANDRA_DATA_DIR" ]; then
        CASSANDRA_DATA_DIR="/storage/cassandra/data"
    else
        CASSANDRA_DATA_DIR="$CASSANDRA_DATA_DIR/cassandra_data"
    fi

    if [ -z "$CASSANDRA_COMMITLOG_DIR" ]; then
        CASSANDRA_COMMITLOG_DIR="/storage/cassandra/commitlog"
    else
        CASSANDRA_COMMITLOG_DIR="$CASSANDRA_COMMITLOG_DIR/cassandra_commitlog"
    fi

    if [ -z "$CASSANDRA_CACHES_DIR" ]; then
        CASSANDRA_CACHES_DIR="/storage/cassandra/saved_caches"
    else
        CASSANDRA_CACHES_DIR="$CASSANDRA_CACHES_DIR/cassandra_caches"
    fi

    echo "[INFO] Cassandra data dir: $CASSANDRA_DATA_DIR"
    echo "[INFO] Cassandra commit log dir: $CASSANDRA_COMMITLOG_DIR"
    echo "[INFO] Cassandra saved caches dir: $CASSANDRA_CACHES_DIR"

    dirs=("$CASSANDRA_DATA_DIR $CASSANDRA_COMMITLOG_DIR $CASSANDRA_CACHES_DIR")

	count=${#dirs[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        directory=${dirs[$i]}

        mkdir -p $directory

        if [ $? -ne 0 ]; then
            terminate "Failed to create directory: $directory"
        fi

        chown -R cassandra:cassandra $directory

        if [ $? -ne 0 ]; then
            terminate "Failed to assign cassandra:cassandra as an owner of directory $directory"
        fi
    done

    DATA_DIR_SPEC="\n"

    dirs=($CASSANDRA_DATA_DIR)

	count=${#dirs[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        dataDir=${dirs[$i]}
        DATA_DIR_SPEC="${DATA_DIR_SPEC}     - ${dataDir}\n"
    done

    CASSANDRA_DATA_DIR=$(echo $DATA_DIR_SPEC | sed -r "s/\//\\\\\//g")
    CASSANDRA_COMMITLOG_DIR=$(echo $CASSANDRA_COMMITLOG_DIR | sed -r "s/\//\\\\\//g")
    CASSANDRA_CACHES_DIR=$(echo $CASSANDRA_CACHES_DIR | sed -r "s/\//\\\\\//g")
}

# Attaches environment configuration settings
. $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/env.sh

# Validates node type of EC2 instance
if [ "$1" != "cassandra" ] && [ "$1" != "ignite" ] && [ "$1" != "test" ] && [ "$1" != "ganglia" ]; then
    echo "[ERROR] Unsupported node type specified: $1"
    exit 1
fi

# Sets node type of EC2 instance
export NODE_TYPE=$1
