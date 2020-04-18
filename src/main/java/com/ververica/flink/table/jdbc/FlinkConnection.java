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

package com.ververica.flink.table.jdbc;

import com.ververica.flink.table.gateway.rest.message.StatementExecuteResponseBody;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.jdbc.rest.RestUtils;
import com.ververica.flink.table.jdbc.rest.SessionClient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Flink JDBC connection.
 */
public class FlinkConnection implements Connection {

	private final SessionClient session;
	private boolean closed;

	public FlinkConnection(String url) throws Exception {
		this.closed = false;
		this.session = createSession(url.substring(FlinkDriver.URL_PREFIX.length()));
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new FlinkStatement(session, this);
	}

	@Override
	public void close() throws SQLException {
		if (closed) {
			return;
		}

		try {
			session.close();
		} catch (Exception e) {
			throw new SQLException(e);
		}
		closed = true;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		session.submitStatement("USE CATALOG " + catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		StatementExecuteResponseBody response;
		response = session.submitStatement("SHOW CURRENT CATALOG");
		Preconditions.checkArgument(
			response.getResults().size() == 1,
			"SHOW CURRENT CATALOG should return exactly one result set. This is a bug.");

		Either<JobID, ResultSet> jobIdOrResultSet =
			RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0));
		Preconditions.checkArgument(
			jobIdOrResultSet.isRight(),
			"SHOW CURRENT CATALOG should immediately return a result. This is a bug.");

		ResultSet resultSet = jobIdOrResultSet.right();
		Preconditions.checkArgument(
			resultSet.getData().size() == 1,
			"SHOW CURRENT CATALOG should return exactly one row of result. This is a bug.");

		return resultSet.getData().get(0).toString();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return new FlinkDatabaseMetaData(session, this);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		session.submitStatement("USE " + schema);
	}

	@Override
	public String getSchema() throws SQLException {
		StatementExecuteResponseBody response;
		response = session.submitStatement("SHOW CURRENT DATABASE");
		Preconditions.checkArgument(
			response.getResults().size() == 1,
			"SHOW CURRENT DATABASE should return exactly one result set. This is a bug.");

		Either<JobID, ResultSet> jobIdOrResultSet =
			RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0));
		Preconditions.checkArgument(
			jobIdOrResultSet.isRight(),
			"SHOW CURRENT DATABASE should immediately return a result. This is a bug.");

		ResultSet resultSet = jobIdOrResultSet.right();
		Preconditions.checkArgument(
			resultSet.getData().size() == 1,
			"SHOW CURRENT DATABASE should return exactly one row of result. This is a bug.");

		return resultSet.getData().get(0).toString();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareCall is not supported");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#nativeSQL is not supported");
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
		return true;
	}

	@Override
	public void commit() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#commit is not supported");
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#rollback is not supported");
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setReadOnly is not supported");
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#isReadOnly is not supported");
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#clearWarnings is not supported");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createStatement is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareCall is not supported");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#getTypeMap is not supported");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setTypeMap is not supported");
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setHoldability is not supported");
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#getHoldability is not supported");
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setSavepoint is not supported");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setSavepoint is not supported");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#rollback is not supported");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#releaseSavepoint is not supported");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createStatement is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareCall is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#prepareStatement is not supported");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createClob is not supported");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createBlob is not supported");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createNClob is not supported");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createSQLXML is not supported");
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		// TODO support timeout
		if (timeout < 0) {
			throw new SQLException("Timeout must not be negative");
		}

		try {
			session.sendHeartbeat();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#getClientInfo is not supported");
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#getClientInfo is not supported");
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createArrayOf is not supported");
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#createStruct is not supported");
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#abort is not supported");
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#setNetworkTimeout is not supported");
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#getNetworkTimeout is not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#unwrap is not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkConnection#isWrapperFor is not supported");
	}

	private UrlInfo parseUrl(String url) {
		String neededParams = "These url parameters are needed: planner";

		String host;
		int port;
		String planner = null;
		Map<String, String> properties = new HashMap<>();

		int argumentStart = url.indexOf('?');
		if (argumentStart < 0) {
			throw new IllegalArgumentException(neededParams);
		} else {
			int colonPos = url.indexOf(':');
			if (colonPos < 0) {
				throw new IllegalArgumentException("Cannot read port from string " + url);
			} else {
				host = url.substring(0, colonPos);
				try {
					port = Integer.valueOf(url.substring(colonPos + 1, argumentStart));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Invalid port format");
				}
			}
		}

		for (String kv : url.substring(argumentStart + 1).split("&")) {
			int equalPos = kv.indexOf('=');
			if (equalPos < 0) {
				throw new IllegalArgumentException("Invalid url parameter kv pair " + kv);
			}

			String key = kv.substring(0, equalPos);
			String value = kv.substring(equalPos + 1);

			if (key.equals("planner")) {
				planner = value;
			} else {
				properties.put(key, value);
			}
		}

		if (planner == null) {
			throw new IllegalArgumentException(neededParams);
		}

		return new UrlInfo(host, port, planner, properties);
	}

	private SessionClient createSession(String url) throws Exception {
		UrlInfo urlInfo = parseUrl(url);
		return new SessionClient(urlInfo.host, urlInfo.port, "Flink-JDBC", urlInfo.planner, "batch", urlInfo.properties, "Flink-JDBC-Connection-IO");
	}

	/**
	 * Contents of Flink JDBC url.
	 */
	private static class UrlInfo {
		final String host;
		final int port;
		final String planner;
		final Map<String, String> properties;

		UrlInfo(String host, int port, String planner, Map<String, String> properties) {
			this.host = host;
			this.port = port;
			this.planner = planner;
			this.properties = properties;
		}
	}
}
