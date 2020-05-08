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
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.jdbc.rest.RestUtils;
import com.ververica.flink.table.jdbc.rest.SessionClient;
import com.ververica.flink.table.jdbc.resulthandler.ResultHandlerFactory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Flink JDBC statement.
 */
public class FlinkStatement implements Statement {

	private static final List<String> QUERY_COMMANDS = Arrays.asList(
		"SELECT",
		"SHOW_MODULES",
		"SHOW_CATALOGS",
		"SHOW_CURRENT_CATALOG",
		"SHOW_DATABASES",
		"SHOW_CURRENT_DATABASE",
		"SHOW_TABLES",
		"SHOW_VIEWS",
		"SHOW_FUNCTIONS",
		"DESCRIBE",
		"EXPLAIN");

	private final SessionClient session;
	private final FlinkConnection connection;

	private final AtomicReference<AtomicStatements> currentStatements;

	private long maxRows;
	private int queryTimeout;
	private int fetchSize;

	private boolean closed;

	public FlinkStatement(SessionClient session, FlinkConnection connection) {
		this.session = session;
		this.connection = connection;

		this.currentStatements = new AtomicReference<>();

		this.maxRows = 0;
		this.queryTimeout = 0;
		this.fetchSize = 0;

		this.closed = false;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		boolean isQuery = execute(sql);
		if (!isQuery) {
			throw new SQLException(sql + " is not a query");
		}
		return getResultSet();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		return (int) executeLargeUpdate(sql);
	}

	@Override
	public void close() throws SQLException {
		if (closed) {
			return;
		}

		cancel();
		closed = true;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#getMaxFieldSize is not supported");
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#setMaxFieldSize is not supported");
	}

	@Override
	public int getMaxRows() throws SQLException {
		return (int) getLargeMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		setLargeMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#setEscapeProcessing is not supported");
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		checkClosed();
		return queryTimeout;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		checkClosed();
		if (seconds < 0) {
			throw new SQLException("Query timeout must not be negative.");
		}

		queryTimeout = seconds;
	}

	@Override
	public void cancel() throws SQLException {
		checkClosed();

		AtomicStatements statements = currentStatements.get();
		if (statements == null) {
			// do nothing
			return;
		}
		statements.cancel();
		currentStatements.set(null);
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
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#setCursorName is not supported");
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		cancel();
		AtomicStatements statements = new AtomicResultSetStatements(sql);
		statements.runNext();
		currentStatements.set(statements);
		return statements.isQuery();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkClosed();
		checkHasStatements();

		AtomicStatements statements = currentStatements.get();
		if (!statements.isQuery()) {
			throw new SQLException("Current result is not a result set. Please call getUpdateCount() instead.");
		}

		Object ret = statements.getCurrentResult();
		if (ret instanceof ResultSet) {
			return (ResultSet) ret;
		} else {
			throw new SQLException("Current result is not a result set.");
		}
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return (int) getLargeUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		checkClosed();
		checkHasStatements();
		AtomicStatements statements = currentStatements.get();
		statements.runNext();
		return statements.isQuery();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#setFetchDirection is not supported");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#getFetchDirection is not supported");
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		checkClosed();
		if (rows < 0) {
			throw new SQLException("Fetch size must not be negative.");
		}

		fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkClosed();
		return fetchSize;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#getResultSetConcurrency is not supported");
	}

	@Override
	public int getResultSetType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#addBatch is not supported");
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#clearBatch is not supported");
	}

	@Override
	public int[] executeBatch() throws SQLException {
		long[] result = executeLargeBatch();
		int[] ret = new int[result.length];
		for (int i = 0; i < result.length; i++) {
			ret[i] = (int) result[i];
		}
		return ret;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#getMoreResults is not supported");
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#getGeneratedKeys is not supported");
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return (int) executeLargeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return (int) executeLargeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return (int) executeLargeUpdate(sql, columnNames);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#execute is not supported");
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#execute is not supported");
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#execute is not supported");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#execute is not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#setPoolable is not supported");
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#isPoolable is not supported");
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#closeOnCompletion is not supported");
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#isCloseOnCompletion is not supported");
	}

	@Override
	public long getLargeUpdateCount() throws SQLException {
		checkClosed();
		checkHasStatements();

		AtomicStatements statements = currentStatements.get();
		if (statements.isQuery()) {
			throw new SQLException("Current result is not an update count. Please call getResultSet() instead.");
		}

		if (statements.afterLastStatement()) {
			// According to the java doc of getMoreResults()
			return -1L;
		} else {
			Object ret = statements.getCurrentResult();
			if (ret instanceof ResultSet) {
				ResultSet rs = (ResultSet) ret;
				if (rs.next()) {
					try {
						if (rs.getString(1).equals(ConstantNames.OK)){
							// if returns only OK
							return 0;
						} else {
							return rs.getLong(1);
						}
					} catch (SQLException e) {
						throw new SQLException("Current result is not an update count.");
					}
				} else {
					throw new SQLException("Current result is not an update count.");
				}
			} else {
				throw new SQLException("Current result is not an update count.");
			}
		}
	}

	@Override
	public void setLargeMaxRows(long max) throws SQLException {
		checkClosed();
		if (max < 0) {
			throw new SQLException("Max rows must not be negative.");
		}

		maxRows = max;
	}

	@Override
	public long getLargeMaxRows() throws SQLException {
		checkClosed();
		return maxRows;
	}

	@Override
	public long[] executeLargeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#executeLargeBatch is not supported");
	}

	@Override
	public long executeLargeUpdate(String sql) throws SQLException {
		boolean isQuery = execute(sql);
		if (isQuery) {
			throw new SQLException(sql + " is not an update statement");
		}
		return getLargeUpdateCount();
	}

	@Override
	public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#executeLargeUpdate is not supported");
	}

	@Override
	public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#executeLargeUpdate is not supported");
	}

	@Override
	public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#executeLargeUpdate is not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#unwrap is not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkStatement#isWrapperFor is not supported");
	}

	private void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException("This result set is already closed");
		}
	}

	private void checkHasStatements() throws SQLException {
		if (currentStatements.get() == null) {
			throw new SQLException("No statement is running");
		}
	}

	/**
	 * A group of statements executed in order,
	 * with atomic read results / change current statement interface.
	 *
	 * @param <R>	statement result type
	 */
	private interface AtomicStatements<R> {

		boolean runNext() throws SQLException;

		R getCurrentResult();

		boolean isQuery();

		boolean afterLastStatement();

		void cancel() throws SQLException;
	}

	/**
	 * A group of statements executed in order,
	 * with atomic read results / change current statement interface.
	 *
	 * <p>These statements produce {@link ResultSet} as results.
	 */
	private class AtomicResultSetStatements implements AtomicStatements<ResultSet> {

		private final String[] statements;
		private int lastExecutedIdx;

		private Either<JobID, com.ververica.flink.table.gateway.rest.result.ResultSet> jobIdOrResultSet;
		private ResultSet currentResultSet;
		private boolean isQuery;

		private ReadWriteLock lock;

		AtomicResultSetStatements(String stmt) {
			this.statements = stmt.split(";");
			this.lastExecutedIdx = -1;

			this.lock = new ReentrantReadWriteLock();
		}

		@Override
		public boolean runNext() throws SQLException {
			lock.writeLock().lock();
			if (lastExecutedIdx < statements.length) {
				lastExecutedIdx++;
			}
			if (lastExecutedIdx >= statements.length) {
				// According to the java doc of getMoreResults()
				isQuery = false;
				lock.writeLock().unlock();
				return false;
			}
			String sql = statements[lastExecutedIdx];

			try {
				StatementExecuteResponseBody response;
				response = queryTimeout > 0 ?
					session.submitStatement(sql, queryTimeout * 1000L) :
					session.submitStatement(sql);

				Preconditions.checkState(
					response.getResults().size() == 1 && response.getStatementTypes().size() == 1,
					"Statement " + sql + " should produce exactly 1 result set. This is a bug.");
				jobIdOrResultSet = RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0));
				currentResultSet = new FlinkResultSet(
					session,
					jobIdOrResultSet,
					ResultHandlerFactory.getResultHandlerByStatementType(response.getStatementTypes().get(0)),
					maxRows,
					FlinkStatement.this);
				currentResultSet.setFetchSize(fetchSize);
				isQuery = QUERY_COMMANDS.contains(response.getStatementTypes().get(0));
				return true;
			} finally {
				lock.writeLock().unlock();
			}
		}

		@Override
		public boolean isQuery() {
			lock.readLock().lock();
			boolean ret = isQuery;
			lock.readLock().unlock();
			return ret;
		}

		@Override
		public boolean afterLastStatement() {
			lock.readLock().lock();
			boolean ret = lastExecutedIdx >= statements.length;
			lock.readLock().unlock();
			return ret;
		}

		@Override
		public ResultSet getCurrentResult() {
			lock.readLock().lock();
			ResultSet ret = currentResultSet;
			lock.readLock().unlock();
			return ret;
		}

		@Override
		public void cancel() throws SQLException {
			lock.writeLock().lock();
			currentResultSet.close();
			lock.writeLock().unlock();
		}
	}
}
