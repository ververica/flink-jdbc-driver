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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link FlinkStatement}.
 *
 * <p>NOTE: Please clean up newly added tables and databases after each test,
 * set current catalog back to default_catalog and set current database back to default_database.
 */
public class FlinkStatementTest {

	private static FlinkJdbcDriverTestingGateway gateway;
	private static Connection connection;
	private Statement statement;

	@BeforeClass
	public static void beforeClass() throws Exception {
		gateway = new FlinkJdbcDriverTestingGateway();
		gateway.start();

		InetSocketAddress addr = gateway.getServerAddress();
		Assert.assertNotNull(addr);
		connection = new FlinkConnection("jdbc:flink://localhost:" + addr.getPort() + "?planner=blink");
		Assert.assertTrue(connection.isValid(0));
	}

	@AfterClass
	public static void afterClass() throws Exception {
		connection.close();
		gateway.stop();
	}

	@Before
	public void before() throws SQLException {
		statement = connection.createStatement();
	}

	@After
	public void after() throws SQLException {
		statement.close();
	}

	@Test
	public void testExecuteQuery() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable ORDER BY a, b LIMIT 2")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(22, rs.getInt(1));
			Assert.assertEquals("BBB Hi", rs.getString(2));

			Assert.assertTrue(rs.next());
			Assert.assertEquals(32, rs.getInt(1));
			Assert.assertEquals("CCC World", rs.getString(2));

			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testExecuteUpdate() throws Exception {
		File tmpFile = File.createTempFile("flink-jdbc-driver-test", ".csv");
		tmpFile.deleteOnExit();

		int createTableUpdateCount = statement.executeUpdate(
			"CREATE TABLE testTable(" +
				"	fa INT," +
				"	fb VARCHAR(100)" +
				") WITH (" +
				"	'connector.type'='filesystem'," +
				"	'connector.path'='file://" + tmpFile.getPath() + "'," +
				"	'format.type' = 'csv')");
		// CREATE TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, createTableUpdateCount);

		int insertUpdateCount = statement.executeUpdate(
			"INSERT INTO testTable VALUES (1, 'stra'), (2, 'strb')");
		// TODO change this when gateway supports real update count
		Assert.assertEquals(Statement.SUCCESS_NO_INFO, insertUpdateCount);

		try (ResultSet rs = statement.executeQuery("SELECT * FROM testTable ORDER BY fa")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(1, rs.getInt("fa"));
			Assert.assertEquals("stra", rs.getString("fb"));

			Assert.assertTrue(rs.next());
			Assert.assertEquals(2, rs.getInt("fa"));
			Assert.assertEquals("strb", rs.getString("fb"));

			Assert.assertFalse(rs.next());
		}

		int dropTableUpdateCount = statement.executeUpdate("DROP TABLE testTable");
		// DROP TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, dropTableUpdateCount);
	}

	@Test
	public void testMultipleStatements() throws Exception {
		File tmpFile1 = File.createTempFile("flink-jdbc-driver-test", ".csv");
		File tmpFile2 = File.createTempFile("flink-jdbc-driver-test", ".csv");
		tmpFile1.deleteOnExit();
		tmpFile2.deleteOnExit();

		boolean executeIsQuery = statement.execute("CREATE TABLE testTable1(" +
			"	fa INT," +
			"	fb VARCHAR(100)" +
			") WITH (" +
			"	'connector.type'='filesystem'," +
			"	'connector.path'='file://" + tmpFile1.getPath() + "'," +
			"	'format.type' = 'csv');" +
			"INSERT INTO testTable1 VALUES (1, 'stra'), (2, 'strb');" +
			"SELECT * FROM testTable1 ORDER BY fa;" +

			"CREATE TABLE testTable2(" +
			"	fc INT," +
			"	fd VARCHAR(100)" +
			") WITH (" +
			"	'connector.type'='filesystem'," +
			"	'connector.path'='file://" + tmpFile2.getPath() + "'," +
			"	'format.type' = 'csv');" +
			"INSERT INTO testTable2(fc, fd) SELECT * FROM testTable1;" +
			"SELECT * FROM testTable2 ORDER BY fc;" +

			"DROP TABLE testTable1;" +
			"DROP TABLE testTable2;");

		Assert.assertFalse(executeIsQuery);
		// CREATE TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, statement.getUpdateCount());

		Assert.assertFalse(statement.getMoreResults());
		// TODO change this when gateway supports real update count
		Assert.assertEquals(Statement.SUCCESS_NO_INFO, statement.getUpdateCount());

		Assert.assertTrue(statement.getMoreResults());
		ResultSet rs1 = statement.getResultSet();
		Assert.assertTrue(rs1.next());
		Assert.assertEquals(1, rs1.getInt("fa"));
		Assert.assertEquals("stra", rs1.getString("fb"));
		Assert.assertTrue(rs1.next());
		Assert.assertEquals(2, rs1.getInt("fa"));
		Assert.assertEquals("strb", rs1.getString("fb"));
		Assert.assertFalse(rs1.next());

		Assert.assertFalse(statement.getMoreResults());
		// CREATE TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, statement.getUpdateCount());

		Assert.assertFalse(statement.getMoreResults());
		// TODO change this when gateway supports real update count
		Assert.assertEquals(Statement.SUCCESS_NO_INFO, statement.getUpdateCount());

		Assert.assertTrue(statement.getMoreResults());
		ResultSet rs2 = statement.getResultSet();
		Assert.assertTrue(rs2.next());
		Assert.assertEquals(1, rs2.getInt("fc"));
		Assert.assertEquals("stra", rs2.getString("fd"));
		Assert.assertTrue(rs2.next());
		Assert.assertEquals(2, rs2.getInt("fc"));
		Assert.assertEquals("strb", rs2.getString("fd"));
		Assert.assertFalse(rs2.next());

		Assert.assertFalse(statement.getMoreResults());
		// DROP TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, statement.getUpdateCount());

		Assert.assertFalse(statement.getMoreResults());
		// DROP TABLE is a DDL, according to JDBC Java doc it's update count is 0
		Assert.assertEquals(0, statement.getUpdateCount());

		Assert.assertFalse(statement.getMoreResults());
		Assert.assertEquals(-1, statement.getUpdateCount());
	}

	@Test
	public void testShows() throws Exception {
		compareStringResultsWithSorting(
			new String[]{"default_catalog", "cat1", "cat2"}, statement.executeQuery("SHOW CATALOGS"));

		statement.execute("USE CATALOG cat1");
		statement.execute("CREATE DATABASE db12");
		compareStringResultsWithSorting(
			new String[]{"db11", "db12"}, statement.executeQuery("SHOW DATABASES"));

		statement.execute("USE db11");
		compareStringResultsWithSorting(new String[]{"cat1"}, statement.executeQuery("SHOW CURRENT CATALOG"));
		compareStringResultsWithSorting(new String[]{"db11"}, statement.executeQuery("SHOW CURRENT DATABASE"));

		File tmpFile1 = File.createTempFile("flink-jdbc-driver-test", ".csv");
		File tmpFile2 = File.createTempFile("flink-jdbc-driver-test", ".csv");
		tmpFile1.deleteOnExit();
		tmpFile2.deleteOnExit();

		statement.executeUpdate("CREATE TABLE testTable1(" +
			"	fa INT," +
			"	fb VARCHAR(100)" +
			") WITH (" +
			"	'connector.type'='filesystem'," +
			"	'connector.path'='file://" + tmpFile1.getPath() + "'," +
			"	'format.type' = 'csv');");
		statement.executeUpdate("CREATE TABLE testTable2(" +
			"	fc INT," +
			"	fd VARCHAR(100)" +
			") WITH (" +
			"	'connector.type'='filesystem'," +
			"	'connector.path'='file://" + tmpFile2.getPath() + "'," +
			"	'format.type' = 'csv');");
		compareStringResultsWithSorting(
			new String[]{"testTable1", "testTable2"}, statement.executeQuery("SHOW TABLES"));

		statement.executeUpdate("DROP TABLE testTable1");
		statement.executeUpdate("DROP TABLE testTable2");
		statement.executeUpdate("DROP DATABASE db12");
		statement.executeUpdate("USE CATALOG default_catalog");
	}

	@Test
	public void testMaxRows() throws SQLException {
		// max rows is smaller than actual result count
		statement.setMaxRows(2);
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable ORDER BY a, b")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(22, rs.getInt(1));
			Assert.assertEquals("BBB Hi", rs.getString(2));
			Assert.assertTrue(rs.next());
			Assert.assertEquals(32, rs.getInt(1));
			Assert.assertEquals("CCC World", rs.getString(2));
			Assert.assertFalse(rs.next());
		}

		// max rows is larger than actual result count
		statement.setMaxRows(5);
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable ORDER BY a, b LIMIT 2")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(22, rs.getInt(1));
			Assert.assertEquals("BBB Hi", rs.getString(2));
			Assert.assertTrue(rs.next());
			Assert.assertEquals(32, rs.getInt(1));
			Assert.assertEquals("CCC World", rs.getString(2));
			Assert.assertFalse(rs.next());
		}
	}

	private void compareStringResultsWithSorting(String[] expected, ResultSet actualResultSet) throws SQLException {
		Arrays.sort(expected);

		List<String> actualList = new ArrayList<>();
		for (int i = 0; i < expected.length; i++) {
			Assert.assertTrue(actualResultSet.next());
			actualList.add(actualResultSet.getString(1));
		}
		String[] actual = actualList.toArray(new String[0]);
		Arrays.sort(actual);

		Assert.assertArrayEquals(expected, actual);
	}
}
