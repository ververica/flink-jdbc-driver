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
import org.junit.Test;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tests for {@link FlinkConnection}.
 */
public class FlinkConnectionTest {

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
		statement.close();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		connection.close();
		gateway.stop();
	}

	@Test
	public void testGetSetCatalog() throws SQLException {
		connection.setCatalog("cat1");
		Assert.assertEquals("cat1", connection.getCatalog());
		connection.setCatalog("cat2");
		Assert.assertEquals("cat2", connection.getCatalog());
	}

	@Test
	public void testGetSetDatabase() throws SQLException {
		connection.setCatalog("cat1");
		Assert.assertEquals("db11", connection.getSchema());
		connection.setSchema("db12");
		Assert.assertEquals("db12", connection.getSchema());
		connection.setCatalog("cat2");
		Assert.assertEquals("db21", connection.getSchema());
		connection.setSchema("db22");
		Assert.assertEquals("db22", connection.getSchema());
	}
}
