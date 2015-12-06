/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.cassandra.utils.datasource;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.AddressTranslater;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSessionImpl;

/**
 * Data source abstraction to specify configuration of the Cassandra session to be used
 */
public class DataSource {
    /** Number of rows to immediately fetch in CQL statement execution. */
    private Integer fetchSize;

    /** Consistency level for READ operations. */
    private ConsistencyLevel readConsistency;

    /** Consistency level for WRITE operations. */
    private ConsistencyLevel writeConsistency;

    /** Username to use for authentication. */
    private String user;

    /** Password to use for authentication. */
    private String pwd;

    /** Port to use for Cassandra connection. */
    private Integer port;

    /** List of contact points to connect to Cassandra cluster. */
    private List<InetAddress> contactPoints;

    /** List of contact points with ports to connect to Cassandra cluster. */
    private List<InetSocketAddress> contactPointsWithPorts;

    /** Maximum time to wait for schema agreement before returning from a DDL query. */
    private Integer maxSchemaAgreementWaitSeconds;

    /** The native protocol version to use. */
    private Integer protoVer;

    /** Compression to use for the transport. */
    private String compression;

    /** Use SSL for communications with Cassandra. */
    private Boolean useSSL;

    /** Enables metrics collection. */
    private Boolean collectMetrix;

    /** Enables JMX reporting of the metrics. */
    private Boolean jmxReporting;

    /** Credentials to use for authentication. */
    private Credentials creds;

    /** Load balancing policy to use. */
    private LoadBalancingPolicy loadBalancingPlc;

    /** Reconnection policy to use. */
    private ReconnectionPolicy reconnectionPlc;

    /** Retry policy to use. */
    private RetryPolicy retryPlc;

    /** Address translator to use. */
    private AddressTranslater addrTranslater;

    /** Speculative execution policy to use. */
    private SpeculativeExecutionPolicy speculativeExecutionPlc;

    /** Authentication provider to use. */
    private AuthProvider authProvider;

    /** SSL options to use. */
    private SSLOptions sslOptions;

    /** Connection pooling options to use. */
    private PoolingOptions poolingOptions;

    /** Socket options to use. */
    private SocketOptions sockOptions;

    /** Netty options to use for connection. */
    private NettyOptions nettyOptions;

    /** Cassandra session wrapper instance. */
    private volatile CassandraSession ses;

    /**
     * Sets user name to use for authentication.
     *
     * @param user user name
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setUser(String user) {
        this.user = user;

        invalidate();
    }

    /**
     * Sets password to use for authentication.
     *
     * @param pwd password
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPassword(String pwd) {
        this.pwd = pwd;

        invalidate();
    }

    /**
     * Sets port to use for Cassandra connection.
     *
     * @param port port
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPort(int port) {
        this.port = port;

        invalidate();
    }

    /**
     * Sets list of contact points to connect to Cassandra cluster.
     *
     * @param points contact points
     */
    public void setContactPoints(String... points) {
        if (points == null || points.length == 0)
            return;

        for (String point : points) {
            if (point.contains(":")) {
                if (contactPointsWithPorts == null)
                    contactPointsWithPorts = new LinkedList<>();

                String[] chunks = point.split(":");

                try {
                    contactPointsWithPorts.add(InetSocketAddress.createUnresolved(chunks[0].trim(), Integer.parseInt(chunks[1].trim())));
                }
                catch (Throwable e) {
                    throw new IllegalArgumentException("Incorrect contact point '" + point + "' specified for Cassandra cache storage", e);
                }
            }
            else {
                if (contactPoints == null)
                    contactPoints = new LinkedList<>();

                try {
                    contactPoints.add(InetAddress.getByName(point));
                }
                catch (Throwable e) {
                    throw new IllegalArgumentException("Incorrect contact point '" + point + "' specified for Cassandra cache storage", e);
                }
            }
        }

        invalidate();
    }

    /** Sets maximum time to wait for schema agreement before returning from a DDL query. */
    @SuppressWarnings("UnusedDeclaration")
    public void setMaxSchemaAgreementWaitSeconds(int seconds) {
        maxSchemaAgreementWaitSeconds = seconds;

        invalidate();
    }

    /**
     * Sets the native protocol version to use.
     *
     * @param ver version number
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setProtocolVersion(int ver) {
        protoVer = ver;

        invalidate();
    }

    /**
     * Sets compression algorithm to use for the transport.
     *
     * @param compression compression algorithm
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setCompression(String compression) {
        this.compression = compression == null || compression.trim().isEmpty() ? null : compression.trim();

        try {
            if (this.compression != null)
                ProtocolOptions.Compression.valueOf(this.compression);
        }
        catch (Throwable e) {
            throw new IgniteException("Incorrect compression '" + compression + "' specified for Cassandra connection", e);
        }

        invalidate();
    }

    /**
     * Enables SSL for communications with Cassandra.
     *
     * @param use flag to enable/disable SSL
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setUseSSL(boolean use) {
        useSSL = use;

        invalidate();
    }

    /**
     * Enables metrics collection.
     *
     * @param collect flag to enable/disable metrics collection
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setCollectMetrix(boolean collect) {
        collectMetrix = collect;

        invalidate();
    }

    /**
     * Enables JMX reporting of the metrics.
     *
     * @param enableReporting flag to enable/disable JMX reporting
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setJmxReporting(boolean enableReporting) {
        jmxReporting = enableReporting;

        invalidate();
    }

    /**
     * Sets number of rows to immediately fetch in CQL statement execution.
     *
     * @param size number of rows to fetch.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setFetchSize(int size) {
        fetchSize = size;

        invalidate();
    }

    /**
     * Set consistency level for READ operations.
     *
     * @param level consistency level.
     */
    public void setReadConsistency(String level) {
        readConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /**
     * Set consistency level for WRITE operations.
     *
     * @param level consistency level.
     */
    public void setWriteConsistency(String level) {
        writeConsistency = parseConsistencyLevel(level);

        invalidate();
    }

    /**
     * Sets credentials to use for authentication.
     *
     * @param creds credentials.
     */
    public void setCredentials(Credentials creds) {
        this.creds = creds;

        invalidate();
    }

    /**
     * Sets load balancing policy.
     *
     * @param plc load balancing policy.
     */
    public void setLoadBalancingPolicy(LoadBalancingPolicy plc) {
        this.loadBalancingPlc = plc;

        invalidate();
    }

    /**
     * Sets reconnection policy.
     *
     * @param plc reconnection policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setReconnectionPolicy(ReconnectionPolicy plc) {
        this.reconnectionPlc = plc;

        invalidate();
    }

    /**
     * Sets retry policy.
     *
     * @param plc retry policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setRetryPolicy(RetryPolicy plc) {
        this.retryPlc = plc;

        invalidate();
    }

    /**
     * Sets address translator.
     *
     * @param translater address translator.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAddressTranslater(AddressTranslater translater) {
        this.addrTranslater = translater;

        invalidate();
    }

    /**
     * Sets speculative execution policy.
     *
     * @param plc speculative execution policy.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy plc) {
        this.speculativeExecutionPlc = plc;
        invalidate();
    }

    /**
     * Sets authentication provider.
     *
     * @param provider authentication provider
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setAuthProvider(AuthProvider provider) {
        this.authProvider = provider;
        invalidate();
    }

    /**
     * Sets SSL options.
     *
     * @param options SSL options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSslOptions(SSLOptions options) {
        this.sslOptions = options;
        invalidate();
    }

    /**
     * Sets pooling options.
     *
     * @param options pooling options to use.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setPoolingOptions(PoolingOptions options) {
        this.poolingOptions = options;
        invalidate();
    }

    /**
     * Sets socket options to use.
     *
     * @param options socket options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setSocketOptions(SocketOptions options) {
        this.sockOptions = options;
        invalidate();
    }

    /**
     * Sets netty options to use.
     *
     * @param options netty options.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void setNettyOptions(NettyOptions options) {
        this.nettyOptions = options;
        invalidate();
    }

    /**
     * Creates Cassandra session wrapper if it wasn't created yet and returns it
     *
     * @param log logger
     * @return Cassandra session wrapper
     */
    @SuppressWarnings("deprecation")
    public synchronized CassandraSession session(IgniteLogger log) {
        if (ses != null)
            return ses;

        Cluster.Builder builder = Cluster.builder();

        if (user != null)
            builder = builder.withCredentials(user, pwd);

        if (port != null)
            builder = builder.withPort(port);

        if (contactPoints != null)
            builder = builder.addContactPoints(contactPoints);

        if (contactPointsWithPorts != null)
            builder = builder.addContactPointsWithPorts(contactPointsWithPorts);

        if (maxSchemaAgreementWaitSeconds != null)
            builder = builder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);

        if (protoVer != null)
            builder = builder.withProtocolVersion(protoVer);

        if (compression != null) {
            try {
                builder = builder.withCompression(ProtocolOptions.Compression.valueOf(compression.trim().toLowerCase()));
            }
            catch (IllegalArgumentException e) {
                throw new IgniteException("Incorrect compression option '" + compression + "' specified for Cassandra connection", e);
            }
        }

        if (useSSL != null && useSSL)
            builder = builder.withSSL();

        if (sslOptions != null)
            builder = builder.withSSL(sslOptions);

        if (collectMetrix != null && !collectMetrix)
            builder = builder.withoutMetrics();

        if (jmxReporting != null && !jmxReporting)
            builder = builder.withoutJMXReporting();

        if (creds != null)
            builder = builder.withCredentials(creds.getUser(), creds.getPassword());

        if (loadBalancingPlc != null)
            builder = builder.withLoadBalancingPolicy(loadBalancingPlc);

        if (reconnectionPlc != null)
            builder = builder.withReconnectionPolicy(reconnectionPlc);

        if (retryPlc != null)
            builder = builder.withRetryPolicy(retryPlc);

        if (addrTranslater != null)
            builder = builder.withAddressTranslater(addrTranslater);

        if (speculativeExecutionPlc != null)
            builder = builder.withSpeculativeExecutionPolicy(speculativeExecutionPlc);

        if (authProvider != null)
            builder = builder.withAuthProvider(authProvider);

        if (poolingOptions != null)
            builder = builder.withPoolingOptions(poolingOptions);

        if (sockOptions != null)
            builder = builder.withSocketOptions(sockOptions);

        if (nettyOptions != null)
            builder = builder.withNettyOptions(nettyOptions);

        return ses = new CassandraSessionImpl(builder, fetchSize, readConsistency, writeConsistency, log);
    }

    /**
     * Parses consistency level provided as string.
     *
     * @param level consistency level string.
     *
     * @return consistency level.
     */
    private ConsistencyLevel parseConsistencyLevel(String level) {
        if (level == null)
            return null;

        try {
            return ConsistencyLevel.valueOf(level.trim().toUpperCase());
        }
        catch (Throwable e) {
            throw new IgniteException("Incorrect consistency level '" + level + "' specified for Cassandra connection", e);
        }
    }

    /**
     * Invalidates session.
     */
    private synchronized void invalidate() {
        ses = null;
    }
}
