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

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Tests for {@link FlinkResultSet}.
 */
public class FlinkResultSetTest {

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
	public void testObjectTypes() throws SQLException {
		checkRepresentation("true", Types.BOOLEAN, true);
		checkRepresentation("CAST('123' AS TINYINT)", Types.TINYINT, (byte) 123);
		checkRepresentation("CAST('123' AS SMALLINT)", Types.SMALLINT, (short) 123);
		checkRepresentation("123", Types.INTEGER, 123);
		checkRepresentation("12300000000", Types.BIGINT, 12300000000L);
		checkRepresentation("CAST('123.45' AS FLOAT)", Types.FLOAT, 123.45f);
		checkRepresentation("1e-1", Types.DOUBLE, 0.1);
		checkRepresentation("CAST('123.45' AS DECIMAL(5, 2))", Types.DECIMAL, BigDecimal.valueOf(123.45));
		checkRepresentation("CAST('hello' as VARCHAR(10))", Types.VARCHAR, "hello");
		checkRepresentation("CAST('foo' as CHAR(5))", Types.CHAR, "foo  ");
		checkRepresentation("CAST('2020-02-11' as DATE)", Types.DATE, LocalDate.of(2020, 2, 11));
		checkRepresentation("CAST('15:43:00.123' AS TIME(3))", Types.TIME, LocalTime.of(15, 43, 0, 123000000));
		checkRepresentation("CAST('2020-02-11 15:43:00.123' AS TIMESTAMP(3))", Types.TIMESTAMP, LocalDateTime.of(2020, 2, 11, 15, 43, 0, 123000000));

		// TODO ExpressionReducer will throw exception
		// checkRepresentation("1.0E0 / 0.0E0", Types.DOUBLE, Double.POSITIVE_INFINITY);
		// checkRepresentation("0.0E0 / 0.0E0", Types.DOUBLE, Double.NaN);
	}

	private void checkRepresentation(String expression, int expectedSqlType, Object expected) throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " + expression)) {
			ResultSetMetaData metadata = rs.getMetaData();
			Assert.assertEquals(1, metadata.getColumnCount());
			Assert.assertEquals(expectedSqlType, metadata.getColumnType(1));
			Assert.assertTrue(rs.next());
			Assert.assertEquals(expected, rs.getObject(1));
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetString() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST('str1' AS CHAR(4)) x, " +
				"CAST('str2' AS VARCHAR(4)), " +
				"CAST('str3' AS BINARY(4)), " +
				"CAST('str4' AS VARBINARY(4)), " +
				"CAST(NULL AS VARCHAR(4))")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals("str1", rs.getString(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals("str2", rs.getString(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals("str3", rs.getString(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals("str4", rs.getString(4));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getString(5));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals("str1", rs.getString("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetBoolean() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"true x, 0, 'hello', '0', CAST(NULL AS BOOLEAN)")) {
			Assert.assertTrue(rs.next());
			Assert.assertTrue(rs.getBoolean(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.getBoolean(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertTrue(rs.getBoolean(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.getBoolean(4));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getString(5));
			Assert.assertTrue(rs.wasNull());
			Assert.assertTrue(rs.getBoolean("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetByte() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(1 AS TINYINT) x, 2, '3', CAST(NULL AS TINYINT)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals((byte) 1, rs.getByte(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals((byte) 2, rs.getByte(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals((byte) 3, rs.getByte(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0, rs.getByte(4));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals((byte) 1, rs.getByte("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetShort() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(1 AS SMALLINT) x, 2, '3', CAST(NULL AS SMALLINT)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals((short) 1, rs.getShort(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals((short) 2, rs.getShort(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals((short) 3, rs.getShort(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0, rs.getShort(4));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals((short) 1, rs.getShort("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetInt() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"1 x, '2', CAST(NULL AS INT)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(1, rs.getInt(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(2, rs.getInt(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0, rs.getInt(3));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(1, rs.getInt("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetLong() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(1 AS BIGINT) x, 2, '3', CAST(NULL AS BIGINT)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(1L, rs.getLong(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(2L, rs.getLong(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(3L, rs.getLong(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0L, rs.getLong(4));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(1L, rs.getLong("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetFloat() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(0.2 AS FLOAT) x, 0.4, '0.8', CAST(NULL AS FLOAT)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(0.2F, rs.getFloat(1), 0F);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0.4F, rs.getFloat(2), 0F);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0.8F, rs.getFloat(3), 0F);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0F, rs.getLong(4), 0F);
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(0.2F, rs.getFloat("x"), 0F);
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetDouble() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(0.2 AS DOUBLE) x, 0.4, '0.8', CAST(NULL AS DOUBLE)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(0.2, rs.getDouble(1), 0D);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0.4, rs.getDouble(2), 0D);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0.8, rs.getDouble(3), 0D);
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(0D, rs.getDouble(4), 0D);
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(0.2, rs.getDouble("x"), 0D);
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetBigDecimal() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST(123.45 AS DECIMAL(5, 2)) x, CAST(NULL AS DECIMAL(5, 2))")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(BigDecimal.valueOf(123.45), rs.getBigDecimal(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getBigDecimal(2));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(BigDecimal.valueOf(123.45), rs.getBigDecimal("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetBytes() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST('str1' AS BINARY(4)) x, " +
				"CAST('str2' AS VARBINARY(4)), " +
				"CAST('str3' AS CHAR(4)), " +
				"CAST('str4' AS VARCHAR(4)), " +
				"CAST(NULL AS BINARY(4))")) {
			Assert.assertTrue(rs.next());
			Assert.assertArrayEquals("str1".getBytes(), rs.getBytes(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertArrayEquals("str2".getBytes(), rs.getBytes(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertArrayEquals("str3".getBytes(), rs.getBytes(3));
			Assert.assertFalse(rs.wasNull());
			Assert.assertArrayEquals("str4".getBytes(), rs.getBytes(4));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getBytes(5));
			Assert.assertTrue(rs.wasNull());
			Assert.assertArrayEquals("str1".getBytes(), rs.getBytes("x"));
			Assert.assertFalse(rs.wasNull());
			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetDate() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST('2020-02-12' AS DATE) x, '2020-02-13', CAST(NULL AS DATE)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(Date.valueOf("2020-02-12"), rs.getDate(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(Date.valueOf("2020-02-13"), rs.getDate(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getBytes(3));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(Date.valueOf("2020-02-12"), rs.getDate("x"));
			Assert.assertFalse(rs.wasNull());

			TimeZone tz = TimeZone.getTimeZone("UTC");
			Assert.assertEquals(
				new Date(ZonedDateTime.of(
					LocalDateTime.of(2020, 2, 12, 0, 0, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getDate("x", Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(
				new Date(ZonedDateTime.of(
					LocalDateTime.of(2020, 2, 13, 0, 0, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getDate(2, Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());

			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetTime() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST('15:20:00' AS TIME) x, '16:20:00', CAST(NULL AS TIME)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(Time.valueOf("15:20:00"), rs.getTime(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(Time.valueOf("16:20:00"), rs.getTime(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getBytes(3));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(Time.valueOf("15:20:00"), rs.getTime("x"));
			Assert.assertFalse(rs.wasNull());

			TimeZone tz = TimeZone.getTimeZone("UTC");
			Assert.assertEquals(
				new Time(ZonedDateTime.of(
					LocalDateTime.of(1970, 1, 1, 15, 20, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getTime("x", Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(
				new Time(ZonedDateTime.of(
					LocalDateTime.of(1970, 1, 1, 16, 20, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getTime(2, Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());

			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testGetTimestamp() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT " +
				"CAST('2020-02-12 15:20:00' AS TIMESTAMP) x, '2020-02-13 16:20:00', CAST(NULL AS TIMESTAMP)")) {
			Assert.assertTrue(rs.next());
			Assert.assertEquals(Timestamp.valueOf("2020-02-12 15:20:00"), rs.getTimestamp(1));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(Timestamp.valueOf("2020-02-13 16:20:00"), rs.getTimestamp(2));
			Assert.assertFalse(rs.wasNull());
			Assert.assertNull(rs.getBytes(3));
			Assert.assertTrue(rs.wasNull());
			Assert.assertEquals(Timestamp.valueOf("2020-02-12 15:20:00"), rs.getTimestamp("x"));
			Assert.assertFalse(rs.wasNull());

			TimeZone tz = TimeZone.getTimeZone("UTC");
			Assert.assertEquals(
				new Timestamp(ZonedDateTime.of(
					LocalDateTime.of(2020, 2, 12, 15, 20, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getTimestamp("x", Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());
			Assert.assertEquals(
				new Timestamp(ZonedDateTime.of(
					LocalDateTime.of(2020, 2, 13, 16, 20, 0),
					tz.toZoneId()).toInstant().toEpochMilli()),
				rs.getTimestamp(2, Calendar.getInstance(tz)));
			Assert.assertFalse(rs.wasNull());

			Assert.assertFalse(rs.next());
		}
	}

	@Test
	public void testPositions() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable LIMIT 2")) {
			Assert.assertTrue(rs.isBeforeFirst());
			Assert.assertFalse(rs.isFirst());
			Assert.assertFalse(rs.isAfterLast());
			Assert.assertEquals(0, rs.getRow());

			Assert.assertTrue(rs.next());
			Assert.assertFalse(rs.isBeforeFirst());
			Assert.assertTrue(rs.isFirst());
			Assert.assertFalse(rs.isAfterLast());
			Assert.assertEquals(1, rs.getRow());

			Assert.assertTrue(rs.next());
			Assert.assertFalse(rs.isBeforeFirst());
			Assert.assertFalse(rs.isFirst());
			Assert.assertFalse(rs.isAfterLast());
			Assert.assertEquals(2, rs.getRow());

			Assert.assertFalse(rs.next());
			Assert.assertFalse(rs.isBeforeFirst());
			Assert.assertFalse(rs.isFirst());
			Assert.assertTrue(rs.isAfterLast());
			Assert.assertEquals(0, rs.getRow());
		}
	}

	@Test
	public void testFetchResultMultipleTimes() throws SQLException {
		int[] expectedInt = new int[]{
			22, 32, 32, 42, 42, 52};
		String[] expectedString = new String[]{
			"BBB Hi", "CCC World", "DDD Hello!!!!", "AAA Hello", "EEE Hi!!!!", "FFF World!!!!"};

		statement.setFetchSize(2);
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable ORDER BY a, b")) {
			for (int i = 0; i < expectedInt.length; i++) {
				Assert.assertTrue(rs.next());
				Assert.assertEquals(i + 1, rs.getRow());
				Assert.assertEquals(expectedInt[i], rs.getInt(1));
				Assert.assertEquals(expectedString[i], rs.getString(2));
			}
			Assert.assertFalse(rs.next());
		}
		statement.setFetchSize(0);
	}

	@Test
	public void testInstantResult() throws SQLException {
		String[] expected = new String[]{
			"default_catalog", "cat1", "cat2"};
		Arrays.sort(expected);

		List<String> actualList = new ArrayList<>();
		try (ResultSet rs = statement.executeQuery("SHOW CATALOGS")) {
			for (int i = 0; i < expected.length; i++) {
				Assert.assertTrue(rs.next());
				actualList.add(rs.getString(1));
			}
		}
		String[] actual = actualList.toArray(new String[0]);
		Arrays.sort(actual);

		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void testEmptyResult() throws SQLException {
		try (ResultSet rs = statement.executeQuery("SELECT * FROM myTable WHERE a = -1")) {
			Assert.assertFalse(rs.next());
			Assert.assertEquals(0, rs.getRow());
		}
	}
}
