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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Flink JDBC driver.
 */
public class FlinkDriver implements Driver {

	public static final String URL_PREFIX = "jdbc:flink://";

	static {
		try {
			java.sql.DriverManager.registerDriver(new FlinkDriver());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		try {
			return acceptsURL(url) ? new FlinkConnection(url) : null;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(URL_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return Integer.valueOf(FlinkDatabaseMetaData.DRIVER_VERSION.split("\\.")[0]);
	}

	@Override
	public int getMinorVersion() {
		return Integer.valueOf(FlinkDatabaseMetaData.DRIVER_VERSION.split("\\.")[1]);
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("FlinkDriver#getParentLogger is not supported");
	}
}
