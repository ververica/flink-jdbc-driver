/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ververica.flink.table.jdbc;

import com.ververica.flink.table.gateway.SessionManager;
import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.context.DefaultContext;
import com.ververica.flink.table.gateway.rest.SqlGatewayEndpoint;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.Objects;

/**
 * A {@link SqlGatewayEndpoint} for the Flink JDBC driver test cases to connect to.
 */
public class FlinkJdbcDriverTestingGateway {

	private static final String DEFAULT_ENVIRONMENT_FILE = "default-env.yaml";
	private static final String TEST_DATA_FILE = "test-data.csv";

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 2;

	private MiniClusterWithClientResource miniClusterWithClientResource;
	private SqlGatewayEndpoint endpoint;

	public void start() throws Exception {
		miniClusterWithClientResource = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(getMiniClusterConfig())
				.setNumberTaskManagers(NUM_TMS)
				.setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
				.build());
		miniClusterWithClientResource.before();
		ClusterClient clusterClient = miniClusterWithClientResource.getClusterClient();

		final URL envUrl = FlinkJdbcDriverTestingGateway.class.getClassLoader().getResource(DEFAULT_ENVIRONMENT_FILE);
		Objects.requireNonNull(envUrl);
		final URL dataUrl = FlinkJdbcDriverTestingGateway.class.getClassLoader().getResource(TEST_DATA_FILE);
		Objects.requireNonNull(dataUrl);
		String schema = FileUtils.readFileUtf8(new File(envUrl.getFile()))
			.replace("$VAR_SOURCE_PATH", dataUrl.getPath());
		Environment env = Environment.parse(schema);

		DefaultContext defaultContext = new DefaultContext(
			env,
			Collections.emptyList(),
			clusterClient.getFlinkConfiguration(),
			new DefaultCLI(clusterClient.getFlinkConfiguration()),
			new DefaultClusterClientServiceLoader());
		SessionManager sessionManager = new SessionManager(defaultContext);

		endpoint = new SqlGatewayEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(getEndpointConfig()),
			sessionManager);
		endpoint.start();
	}

	public InetSocketAddress getServerAddress() {
		return endpoint.getServerAddress();
	}

	public void stop() throws Exception {
		endpoint.close();
		miniClusterWithClientResource.after();
	}

	private static Configuration getMiniClusterConfig() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}

	private static Configuration getEndpointConfig() {
		Configuration config = new Configuration();
		config.setString(RestOptions.ADDRESS, "localhost");
		config.setString(RestOptions.BIND_PORT, "0-65535");
		return config;
	}
}
