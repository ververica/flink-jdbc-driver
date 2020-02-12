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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tests for {@link FlinkDatabaseMetaData}.
 */
public class FlinkDatabaseMetaDataTest {

	private static FlinkJdbcDriverTestingGateway gateway;
	private static Connection connection;

	@BeforeClass
	public static void beforeClass() throws Exception {
		gateway = new FlinkJdbcDriverTestingGateway();
		gateway.start();

		InetSocketAddress addr = gateway.getServerAddress();
		Assert.assertNotNull(addr);
		connection = new FlinkConnection("jdbc:flink://localhost:" + addr.getPort() + "?planner=blink");
		Assert.assertTrue(connection.isValid(0));

		Statement statement = connection.createStatement();
		statement.execute("USE CATALOG cat1");
		statement.execute("CREATE DATABASE db12");
		statement.execute("USE CATALOG cat2");
		statement.execute("CREATE DATABASE db22");

		createTmpTable("default_catalog", "default_database", "tab001", statement);
		createTmpTable("default_catalog", "default_database", "tab002", statement);
		createTmpTable("cat1", "db11", "tab111", statement);
		createTmpTable("cat1", "db11", "tab112", statement);
		createTmpTable("cat1", "db12", "tab121", statement);
		createTmpTable("cat2", "db21", "tab211", statement);
		createTmpTable("cat2", "db22", "tab221", statement);
		createTmpTable("cat2", "db22", "tab222", statement);

		runStatementInCatalogAndDatabase(
			"cat1", "db12", "CREATE VIEW view122 AS SELECT * FROM tab121", statement);
		runStatementInCatalogAndDatabase(
			"cat2", "db21", "CREATE VIEW view212 AS SELECT * FROM tab211", statement);

		statement.close();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		connection.close();
		gateway.stop();
	}

	@Test
	public void testGetCatalogs() throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();

		String[][] expected = new String[][] {
			new String[]{"cat1"},
			new String[]{"cat2"},
			new String[]{"default_catalog"}};
		compareStringResults(expected, meta.getCatalogs());
	}

	@Test
	public void testGetSchemas() throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();

		String[][] expected1 = new String[][] {
			new String[]{"db11", "cat1"},
			new String[]{"db12", "cat1"},
			new String[]{"db21", "cat2"},
			new String[]{"db22", "cat2"},
			new String[]{"default_database", "default_catalog"}};
		compareStringResults(expected1, meta.getSchemas());

		String[][] expected2 = new String[][] {
			new String[]{"db12", "cat1"},
			new String[]{"db22", "cat2"}};
		compareStringResults(expected2, meta.getSchemas(null, "d%2"));

		String[][] expected3 = new String[][] {
			new String[]{"db21", "cat2"}};
		compareStringResults(expected3, meta.getSchemas("cat2", "d__1"));
	}

	// view in SQL gateway is not bounded to a certain database, this is a gateway bug
	@Ignore
	@Test
	public void testGetTables() throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();

		String[][] expected1 = new String[][] {
			new String[]{"cat1", "db11", "tab111", "TABLE"},
			new String[]{"cat1", "db11", "tab112", "TABLE"},
			new String[]{"cat1", "db12", "tab121", "TABLE"},
			new String[]{"cat1", "db12", "view122", "VIEW"}};
		compareStringResults(expected1, meta.getTables("cat1", null, null, null));

		String[][] expected2 = new String[][] {
			new String[]{"cat2", "db11", "tab111", "TABLE"},
			new String[]{"cat2", "db12", "tab121", "TABLE"}};
		compareStringResults(expected2, meta.getTables("cat2", null, "t%1", new String[]{"TABLE"}));

		String[][] expected3 = new String[][] {
			new String[]{"cat1", "db12", "view122", "VIEW"}};
		compareStringResults(expected2, meta.getTables("cat2", "d__2", "%2", new String[]{"VIEW"}));
	}

	@Test
	public void testGetColumns() throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();

		String[][] expected1 = new String[][] {
			new String[]{"cat1", "db11", "tab112", "fa"},
			new String[]{"cat2", "db22", "tab222", "fa"},
			new String[]{"default_catalog", "default_database", "tab002", "fa"}};
		compareStringResults(expected1, meta.getColumns(null, null, "t%2", "_a"));

		String[][] expected2 = new String[][] {
			new String[]{"cat2", "db21", "tab211", "fb"}};
		compareStringResults(expected2, meta.getColumns("cat2", "%1", "t%1", "fb"));
	}

	private void compareStringResults(String[][] expected, ResultSet rs) throws SQLException {
		for (String[] row : expected) {
			Assert.assertTrue(rs.next());
			for (int i = 0; i < row.length; i++) {
				Assert.assertEquals(row[i], rs.getString(i + 1));
			}
		}
		Assert.assertFalse(rs.next());
	}

	private static void createTmpTable(
			String catalog, String database, String table, Statement statement) throws Exception {
		statement.execute("USE CATALOG " + catalog);
		statement.execute("USE " + database);

		File tmpFile = File.createTempFile("Flink-JDBC-test", ".csv");
		tmpFile.deleteOnExit();
		statement.execute("CREATE TABLE " + table + "(" +
			"	fa INT," +
			"	fb VARCHAR(100)" +
			") WITH (" +
			"	'connector.type'='filesystem'," +
			"	'connector.path'='file://" + tmpFile.getPath() + "'," +
			"	'format.type' = 'csv')");
	}

	private static void runStatementInCatalogAndDatabase(
			String catalog, String database, String stmt, Statement statement) throws SQLException {
		statement.execute("USE CATALOG " + catalog);
		statement.execute("USE " + database);
		statement.execute(stmt);
	}
}
