/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.jdbc.rest;

import com.ververica.flink.table.gateway.rest.handler.GetInfoHeaders;
import com.ververica.flink.table.gateway.rest.handler.JobCancelHeaders;
import com.ververica.flink.table.gateway.rest.handler.ResultFetchHeaders;
import com.ververica.flink.table.gateway.rest.handler.SessionCloseHeaders;
import com.ververica.flink.table.gateway.rest.handler.SessionCreateHeaders;
import com.ververica.flink.table.gateway.rest.handler.SessionHeartbeatHeaders;
import com.ververica.flink.table.gateway.rest.handler.StatementExecuteHeaders;
import com.ververica.flink.table.gateway.rest.message.GetInfoResponseBody;
import com.ververica.flink.table.gateway.rest.message.ResultFetchMessageParameters;
import com.ververica.flink.table.gateway.rest.message.ResultFetchRequestBody;
import com.ververica.flink.table.gateway.rest.message.ResultFetchResponseBody;
import com.ververica.flink.table.gateway.rest.message.SessionCreateRequestBody;
import com.ververica.flink.table.gateway.rest.message.SessionJobMessageParameters;
import com.ververica.flink.table.gateway.rest.message.SessionMessageParameters;
import com.ververica.flink.table.gateway.rest.message.StatementExecuteRequestBody;
import com.ververica.flink.table.gateway.rest.message.StatementExecuteResponseBody;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExecutorUtils;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A client to connect to Flink SQL gateway.
 */
public class SessionClient {

	private final String serverHost;
	private final int serverPort;
	private final String sessionName;
	private final String planner;
	private final String executionType;
	private final Map<String, String> properties;
	private final RestClient restClient;

	private final ExecutorService executor;
	private volatile String sessionId;
	private volatile boolean isClosed = false;

	public SessionClient(
		String serverHost,
		int serverPort,
		String sessionName,
		String planner,
		String executionType,
		Map<String, String> properties,
		String threadName)
		throws Exception {
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.sessionName = sessionName;
		this.planner = planner;
		this.executionType = executionType;
		this.properties = properties;
		this.executor = Executors.newFixedThreadPool(4, new ExecutorThreadFactory(threadName));
		this.restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), executor);

		connectInternal();
	}

	public String getServerHost() {
		return serverHost;
	}

	public int getServerPort() {
		return serverPort;
	}

	public String getPlanner() {
		return planner;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	private void connectInternal() throws Exception {
		this.sessionId = restClient.sendRequest(
			serverHost,
			serverPort,
			SessionCreateHeaders.getInstance(),
			EmptyMessageParameters.getInstance(),
			new SessionCreateRequestBody(sessionName, planner, executionType, properties))
			.get().getSessionId();
	}

	public synchronized void close() throws Exception {
		if (isClosed) {
			return;
		}
		isClosed = true;
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				SessionCloseHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				EmptyRequestBody.getInstance()).get();
		} finally {
			restClient.shutdown(Time.seconds(5));
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	public synchronized void sendHeartbeat() throws SQLException {
		checkState();
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				SessionHeartbeatHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SQLException("Failed to send heartbeat to server", e);
		}
	}

	public StatementExecuteResponseBody submitStatement(String stmt) throws SQLException {
		return submitStatement(stmt, Long.MAX_VALUE);
	}

	public synchronized StatementExecuteResponseBody submitStatement(String stmt, long executionTimeoutMillis)
		throws SQLException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				StatementExecuteHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				new StatementExecuteRequestBody(stmt, executionTimeoutMillis))
				.get();
		} catch (Exception e) {
			throw new SQLException("Failed to submit statement `" + stmt + "` to server", e);
		}
	}

	public synchronized void cancelJob(JobID jobId) throws SQLException {
		checkState();
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				JobCancelHeaders.getInstance(),
				new SessionJobMessageParameters(sessionId, jobId),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SQLException("Failed to cancel job " + jobId.toString(), e);
		}
	}

	public synchronized ResultFetchResponseBody fetchResult(JobID jobId, long token) throws SQLException {
		return fetchResult(jobId, token, null);
	}

	public synchronized ResultFetchResponseBody fetchResult(
			JobID jobId, long token, Integer fetchSize) throws SQLException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				ResultFetchHeaders.getInstance(),
				new ResultFetchMessageParameters(sessionId, jobId, token),
				new ResultFetchRequestBody(fetchSize))
				.get();
		} catch (Exception e) {
			throw new SQLException(
				"Failed to fetch result for job " + jobId.toString() +
					" (token = " + token + ", fetchSize = " + fetchSize + ")",
				e.getCause());
		}
	}

	public GetInfoResponseBody getInfo() throws SQLException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				GetInfoHeaders.getInstance(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SQLException("Failed to get server info", e);
		}
	}

	private void checkState() {
		if (isClosed) {
			throw new IllegalStateException("Session is already closed.");
		}
	}
}
