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

import com.ververica.flink.table.gateway.rest.message.ResultFetchResponseBody;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.jdbc.rest.SessionClient;
import com.ververica.flink.table.jdbc.resulthandler.ResultHandler;
import com.ververica.flink.table.jdbc.resulthandler.ResultHandlerFactory;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Flink JDBC result set.
 */
public class FlinkResultSet implements ResultSet {

	// If an empty array is fetched as result, at least sleep this long millis before next attempt
	private static final long DEFAULT_INIT_SLEEP_MILLIS = 200L;
	// If an empty array is fetched as result, at most sleep this long millis before next attempt
	private static final long DEFAULT_MAX_SLEEP_MILLIS = 60000L;
	// If an empty array is fetched as result, at most sleep [query elapsed time] * this fraction before next attempt
	private static final double DEFAULT_MAX_SLEEP_FRACTION = 0.1;

	private final SessionClient session;
	private final Either<JobID, com.ververica.flink.table.gateway.rest.result.ResultSet> jobIdOrResultSet;
	private final ResultHandler resultHandler;
	private int fetchSize;
	private final long maxRows;
	private final FlinkStatement statement;

	private final AtomicRowData rowData;

	private boolean wasNull;
	private boolean closed;

	private long resultSetCreateMillis;

	public FlinkResultSet(
			SessionClient session,
			Either<JobID, com.ververica.flink.table.gateway.rest.result.ResultSet> jobIdOrResultSet,
			ResultHandler resultHandler,
			long maxRows,
			FlinkStatement statement) throws SQLException {
		this.session = session;
		this.jobIdOrResultSet = jobIdOrResultSet;
		this.resultHandler = resultHandler;
		this.fetchSize = 0;
		this.maxRows = maxRows;
		this.statement = statement;

		this.rowData = new AtomicRowData();

		this.wasNull = false;
		this.closed = false;

		this.resultSetCreateMillis = System.currentTimeMillis();
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();
		return rowData.nextRow();
	}

	@Override
	public synchronized void close() throws SQLException {
		if (closed) {
			return;
		}

		if (jobIdOrResultSet.isLeft() && rowData.hasMoreResponse()) {
			// no need to lock, closing while fetching new results should throw exception
			session.cancelJob(jobIdOrResultSet.left());
		}
		closed = true;
	}

	@Override
	public boolean wasNull() throws SQLException {
		checkClosed();
		return wasNull;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof byte[]) {
			return new String((byte[]) o);
		} else {
			return o.toString();
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return false;
		} else if (o instanceof Boolean) {
			return (Boolean) o;
		} else if (o instanceof Number) {
			return ((Number) o).intValue() != 0;
		} else if (o instanceof String) {
			// we follow Hive's implementation here, might not be very standard
			return !(o.equals("0"));
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).byteValue();
		} else if (o instanceof String) {
			try {
				return Byte.parseByte((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to byte");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to byte");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).shortValue();
		} else if (o instanceof String) {
			try {
				return Short.parseShort((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to short");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to short");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).intValue();
		} else if (o instanceof String) {
			try {
				return Integer.parseInt((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to int");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to int");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).longValue();
		} else if (o instanceof String) {
			try {
				return Long.parseLong((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to long");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to long");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).floatValue();
		} else if (o instanceof String) {
			try {
				return Float.parseFloat((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to float");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to float");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return 0;
		} else if (o instanceof Number) {
			return ((Number) o).doubleValue();
		} else if (o instanceof String) {
			try {
				return Double.parseDouble((String) o);
			} catch (NumberFormatException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to double");
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to double");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof BigDecimal) {
			return ((BigDecimal) o).setScale(scale, RoundingMode.HALF_EVEN);
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to big decimal");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof byte[]) {
			return (byte[]) o;
		} else if (o instanceof String) {
			return ((String) o).getBytes();
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to bytes");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof Date) {
			return (Date) o;
		} else if (o instanceof LocalDate) {
			return Date.valueOf((LocalDate) o);
		} else if (o instanceof String) {
			try {
				return Date.valueOf((String) o);
			} catch (IllegalArgumentException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to date", e);
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to date");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof Time) {
			return (Time) o;
		} else if (o instanceof LocalTime) {
			return Time.valueOf((LocalTime) o);
		} else if (o instanceof String) {
			try {
				return Time.valueOf((String) o);
			} catch (IllegalArgumentException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to time", e);
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to time");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof Timestamp) {
			return (Timestamp) o;
		} else if (o instanceof LocalDateTime) {
			return Timestamp.valueOf((LocalDateTime) o);
		} else if (o instanceof OffsetDateTime) {
			return Timestamp.valueOf(((OffsetDateTime) o).toLocalDateTime());
		} else if (o instanceof Instant) {
			return Timestamp.from((Instant) o);
		} else if (o instanceof String) {
			try {
				return Timestamp.valueOf((String) o);
			} catch (IllegalArgumentException e) {
				throw new SQLException("Cannot convert column " + columnIndex + " to timestamp", e);
			}
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to timestamp");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getAsciiStream is not supported");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getUnicodeStream is not supported");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getBinaryStream is not supported");
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return getAsciiStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return getUnicodeStream(findColumn(columnLabel));
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return getBinaryStream(findColumn(columnLabel));
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
	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getCursorName is not supported");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new FlinkResultSetMetaData(rowData.getColumnInfos());
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getColumnValue(columnIndex);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		checkClosed();

		List<ColumnInfo> columnInfos = rowData.getColumnInfos();
		for (int i = 0; i < columnInfos.size(); i++) {
			if (columnInfos.get(i).getName().equals(columnLabel)) {
				return i + 1;
			}
		}

		throw new SQLException("Column label " + columnLabel + " not found");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getCharacterStream is not supported");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getCharacterStream is not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		Object o = getColumnValue(columnIndex);
		if (o == null) {
			return null;
		} else if (o instanceof BigDecimal) {
			return (BigDecimal) o;
		}
		throw new SQLException("Cannot convert column " + columnIndex + " to big decimal");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return rowData.getRowCount() == 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return rowData.isAfterLast();
	}

	@Override
	public boolean isFirst() throws SQLException {
		return rowData.getRowCount() == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#isLast is not supported");
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#beforeFirst is not supported");
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#afterLast is not supported");
	}

	@Override
	public boolean first() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#first is not supported");
	}

	@Override
	public boolean last() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#last is not supported");
	}

	@Override
	public int getRow() throws SQLException {
		if (rowData.isAfterLast()) {
			return 0;
		} else {
			return (int) rowData.getRowCount();
		}
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#absolute is not supported");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#relative is not supported");
	}

	@Override
	public boolean previous() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#previous is not supported");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new SQLFeatureNotSupportedException("Flink JDBC only supports ResultSet.FETCH_FORWARD");
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		if (rows < 0) {
			throw new SQLException("Fetch size must not be negative");
		}
		fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return fetchSize;
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getConcurrency is not supported");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNull is not supported");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBoolean is not supported");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateByte is not supported");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateShort is not supported");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateInt is not supported");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateLong is not supported");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateFloat is not supported");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateDouble is not supported");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBigDecimal is not supported");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateString is not supported");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBytes is not supported");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateDate is not supported");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateTime is not supported");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateTimestamp is not supported");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateAsciiStream is not supported");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBinaryStream is not supported");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateCharacterStream is not supported");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateObject is not supported");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateObject is not supported");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		updateNull(findColumn(columnLabel));
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		updateBoolean(findColumn(columnLabel), x);
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		updateByte(findColumn(columnLabel), x);
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		updateShort(findColumn(columnLabel), x);
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		updateInt(findColumn(columnLabel), x);
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		updateLong(findColumn(columnLabel), x);
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		updateFloat(findColumn(columnLabel), x);
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		updateDouble(findColumn(columnLabel), x);
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		updateBigDecimal(findColumn(columnLabel), x);
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		updateString(findColumn(columnLabel), x);
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		updateBytes(findColumn(columnLabel), x);
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		updateDate(findColumn(columnLabel), x);
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		updateTime(findColumn(columnLabel), x);
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		updateTimestamp(findColumn(columnLabel), x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		updateAsciiStream(findColumn(columnLabel), x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		updateBinaryStream(findColumn(columnLabel), x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		updateCharacterStream(findColumn(columnLabel), reader, length);
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		updateObject(findColumn(columnLabel), x, scaleOrLength);
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		updateObject(findColumn(columnLabel), x);
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#insertRow is not supported");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateRow is not supported");
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#deleteRow is not supported");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#refreshRow is not supported");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#cancelRowUpdates is not supported");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#moveToInsertRow is not supported");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#moveToCurrentRow is not supported");
	}

	@Override
	public Statement getStatement() throws SQLException {
		return statement;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getRef is not supported");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getBlob is not supported");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getClob is not supported");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getArray is not supported");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getRef is not supported");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getBlob is not supported");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getClob is not supported");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getArray is not supported");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		Date date = getDate(columnIndex);
		if (date == null) {
			return null;
		} else {
			return new Date(
				new Timestamp(date.getTime())
					.toLocalDateTime()
					.atZone(cal.getTimeZone().toZoneId())
					.toInstant()
					.toEpochMilli());
		}
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		return getDate(findColumn(columnLabel), cal);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		Time time = getTime(columnIndex);
		if (time == null) {
			return null;
		} else {
			return new Time(
				new Timestamp(time.getTime())
					.toLocalDateTime()
					.atZone(cal.getTimeZone().toZoneId())
					.toInstant()
					.toEpochMilli());
		}
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		return getTime(findColumn(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		Timestamp timestamp = getTimestamp(columnIndex);
		if (timestamp == null) {
			return null;
		} else {
			return new Timestamp(
				timestamp
					.toLocalDateTime()
					.atZone(cal.getTimeZone().toZoneId())
					.toInstant()
					.toEpochMilli());
		}
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		return getTimestamp(findColumn(columnLabel), cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getURL is not supported");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getURL is not supported");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateRef is not supported");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		updateRef(findColumn(columnLabel), x);
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBlob is not supported");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		updateBlob(findColumn(columnLabel), x);
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateClob is not supported");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		updateClob(findColumn(columnLabel), x);
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateArray is not supported");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		updateArray(findColumn(columnLabel), x);
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getRowId is not supported");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return getRowId(findColumn(columnLabel));
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateRowId is not supported");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		updateRowId(findColumn(columnLabel), x);
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getHoldability is not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNString is not supported");
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		updateNString(findColumn(columnLabel), nString);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNClob is not supported");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		updateNClob(findColumn(columnLabel), nClob);
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getNClob is not supported");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return getNClob(findColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getSQLXML is not supported");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return getSQLXML(findColumn(columnLabel));
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateSQLXML is not supported");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		updateSQLXML(findColumn(columnLabel), xmlObject);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getNString is not supported");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getNString(findColumn(columnLabel));
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getNCharacterStream is not supported");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getNCharacterStream(findColumn(columnLabel));
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNCharacterStream is not supported");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		updateNCharacterStream(findColumn(columnLabel), reader, length);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateAsciiStream is not supported");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBinaryStream is not supported");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateCharacterStream is not supported");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		updateAsciiStream(findColumn(columnLabel), x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		updateBinaryStream(findColumn(columnLabel), x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		updateCharacterStream(findColumn(columnLabel), reader, length);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBlob is not supported");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		updateBlob(findColumn(columnLabel), inputStream, length);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateClob is not supported");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		updateClob(findColumn(columnLabel), reader, length);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNClob is not supported");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		updateNClob(findColumn(columnLabel), reader, length);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNCharacterStream is not supported");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		updateNCharacterStream(findColumn(columnLabel), reader);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateAsciiStream is not supported");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBinaryStream is not supported");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateCharacterStream is not supported");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		updateAsciiStream(findColumn(columnLabel), x);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		updateBinaryStream(findColumn(columnLabel), x);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		updateCharacterStream(findColumn(columnLabel), reader);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateBlob is not supported");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		updateBlob(findColumn(columnLabel), inputStream);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateClob is not supported");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		updateClob(findColumn(columnLabel), reader);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#updateNClob is not supported");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		updateNClob(findColumn(columnLabel), reader);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#getObject is not supported");
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(findColumn(columnLabel), type);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#unwrap is not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSet#isWrapperFor is not supported");
	}

	public static FlinkResultSet of(com.ververica.flink.table.gateway.rest.result.ResultSet resultSet) throws SQLException {
		return new FlinkResultSet(
			null, Either.Right(resultSet), ResultHandlerFactory.getDefaultResultHandler(), 0L, null);
	}

	private Object getColumnValue(int columnIndex) throws SQLException {
		checkClosed();
		checkHasData();
		checkIndexBound(columnIndex);

		Object o = rowData.getCurrentRow().getField(columnIndex - 1);
		wasNull = (o == null);
		return o;
	}

	private void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException("This result set is already closed");
		}
	}

	private void checkHasData() throws SQLException {
		if (rowData.getRowCount() == 0) {
			throw new SQLException(
				"This result set is pointing before the first result. Please call next() first.");
		}
		if (rowData.isAfterLast()) {
			throw new SQLException(
				"This result set is pointing after the last result. No more results will be provided.");
		}
	}

	private void checkIndexBound(int columnIndex) throws SQLException {
		int columnNum = rowData.getColumnInfos().size();
		if (columnIndex <= 0) {
			throw new SQLException("Column index must be positive.");
		}
		if (columnIndex > columnNum) {
			throw new SQLException(
				"Column index " + columnIndex + " out of bound. There are only " + columnNum + " columns.");
		}
	}

	/**
	 * An atomic iterator-like data structure which read results from SQL gateway.
	 */
	private class AtomicRowData {

		private Row currentRow;
		private int currentIdxInResponse;
		// rowCount = Long.MAX_VALUE indicates that it is pointing after the last result
		private long rowCount;

		private long currentToken;
		private com.ververica.flink.table.gateway.rest.result.ResultSet currentResultSet;
		private boolean hasMoreResponse;

		private final List<ColumnInfo> columnInfos;

		private ReadWriteLock lock;

		AtomicRowData() throws SQLException {
			this.currentIdxInResponse = -1;
			this.rowCount = 0L;

			this.currentToken = -1L;
			this.hasMoreResponse = true;

			this.lock = new ReentrantReadWriteLock();

			// we fetch the first response here to get column infos
			fetchNextResponse(false);
			columnInfos = currentResultSet.getColumns();
		}

		boolean nextRow() throws SQLException {
			if (noMoreRows()) {
				rowCount = Long.MAX_VALUE;
				return false;
			}

			lock.writeLock().lock();
			try {
				if (currentIdxInResponse + 1 < currentResultSet.getData().size()) {
					// current batch of results hasn't been consumed
					currentIdxInResponse++;
					currentRow = currentResultSet.getData().get(currentIdxInResponse);
					rowCount++;
					return true;
				} else if (fetchNextResponse(true)) {
					// a new batch of results arrives
					currentIdxInResponse = 0;
					currentRow = currentResultSet.getData().get(0);
					rowCount++;
					return true;
				} else {
					// no more results
					rowCount = Long.MAX_VALUE;
					return false;
				}
			} finally {
				lock.writeLock().unlock();
			}
		}

		boolean noMoreRows() {
			lock.readLock().lock();
			boolean ret = (maxRows > 0 && rowCount + 1 > maxRows) || !hasMoreResponse;
			lock.readLock().unlock();
			return ret;
		}

		boolean isAfterLast() {
			lock.readLock().lock();
			boolean ret = rowCount == Long.MAX_VALUE;
			lock.readLock().unlock();
			return ret;
		}

		boolean hasMoreResponse() {
			return hasMoreResponse;
		}

		Row getCurrentRow() {
			lock.readLock().lock();
			Row ret = currentRow;
			lock.readLock().unlock();
			return ret;
		}

		long getRowCount() {
			lock.readLock().lock();
			long ret = rowCount;
			lock.readLock().unlock();
			return ret;
		}

		List<ColumnInfo> getColumnInfos() {
			return columnInfos;
		}

		private boolean fetchNextResponse(boolean needData) throws SQLException {
			// a quick check for more response
			if (!hasMoreResponse) {
				return false;
			}

			if (jobIdOrResultSet.isRight()) {
				// we can get results directly
				if (currentResultSet == null) {
					currentResultSet = resultHandler.handleResult(jobIdOrResultSet.right());
					hasMoreResponse = true;
				} else {
					hasMoreResponse = false;
				}
				return hasMoreResponse;
			}

			// do the actual remote fetching work
			long sleepMillis = DEFAULT_INIT_SLEEP_MILLIS;
			while (true) {
				currentToken++;
				ResultFetchResponseBody response;
				if (fetchSize > 0) {
					response = session.fetchResult(jobIdOrResultSet.left(), currentToken, fetchSize);
				} else {
					response = session.fetchResult(jobIdOrResultSet.left(), currentToken);
				}
				hasMoreResponse = (response.getNextResultUri() != null);

				if (!hasMoreResponse) {
					// no more response
					return false;
				}

				// response contains data
				currentResultSet = response.getResults().get(0);
				if (currentResultSet.getData().isEmpty() && needData) {
					// empty array as result but we need data, sleep before next attempt
					try {
						Thread.sleep(sleepMillis);
						long elapsedMillis = System.currentTimeMillis() - resultSetCreateMillis;
						long maxSleepMillis = Math.min(
							DEFAULT_MAX_SLEEP_MILLIS, Math.round(elapsedMillis * DEFAULT_MAX_SLEEP_FRACTION));
						sleepMillis = Math.min(sleepMillis * 2, maxSleepMillis);
						// we do not want the sleep time to be too short, so we should have a lower bound
						sleepMillis = Math.max(sleepMillis, DEFAULT_INIT_SLEEP_MILLIS);
					} catch (InterruptedException e) {
						throw new SQLException(
							"Interrupted while fetching more results for job " + jobIdOrResultSet.left(), e);
					}
				} else {
					// we get a new result set, possibly empty if we don't need data
					currentResultSet = resultHandler.handleResult(currentResultSet);
					break;
				}
			}

			return true;
		}
	}
}
