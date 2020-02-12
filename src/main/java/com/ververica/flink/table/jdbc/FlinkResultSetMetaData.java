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

import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.jdbc.type.FlinkSqlTypes;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Fink JDBC result set meta data.
 */
public class FlinkResultSetMetaData implements ResultSetMetaData {

	private final List<ColumnInfo> columns;

	public FlinkResultSetMetaData(List<ColumnInfo> columns) {
		this.columns = columns;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return columns.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isAutoIncrement is not supported");
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isCaseSensitive is not supported");
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		checkIndexBound(column);
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isCurrency is not supported");
	}

	@Override
	public int isNullable(int column) throws SQLException {
		checkIndexBound(column);
		return columns.get(column - 1).getLogicalType().isNullable() ?
			ResultSetMetaData.columnNullable :
			ResultSetMetaData.columnNoNulls;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isSigned is not supported");
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		checkIndexBound(column);
		return FlinkSqlTypes.getType(columns.get(column - 1).getLogicalType()).getDisplaySize();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		checkIndexBound(column);
		return columns.get(column - 1).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#getSchemaName is not supported");
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		checkIndexBound(column);
		return FlinkSqlTypes.getType(columns.get(column - 1).getLogicalType()).getPrecision();
	}

	@Override
	public int getScale(int column) throws SQLException {
		checkIndexBound(column);
		return FlinkSqlTypes.getType(columns.get(column - 1).getLogicalType()).getScale();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#getTableName is not supported");
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#getCatalogName is not supported");
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		checkIndexBound(column);
		return FlinkSqlTypes.getType(columns.get(column - 1).getLogicalType()).getSqlType();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		checkIndexBound(column);
		return columns.get(column - 1).getType();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isReadOnly is not supported");
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isWritable is not supported");
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isDefinitelyWritable is not supported");
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		checkIndexBound(column);
		return columns.get(column - 1).getLogicalType().getDefaultConversion().getName();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#unwrap is not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkResultSetMetaData#isWrapperFor is not supported");
	}

	private void checkIndexBound(int column) throws SQLException {
		int columnNum = columns.size();
		if (column <= 0) {
			throw new SQLException("Column index must be positive.");
		}
		if (column > columnNum) {
			throw new SQLException(
				"Column index " + column + " out of bound. There are only " + columnNum + " columns.");
		}
	}
}
