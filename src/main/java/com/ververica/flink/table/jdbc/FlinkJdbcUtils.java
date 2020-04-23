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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Util class for Flink JDBC.
 */
public class FlinkJdbcUtils {

	public static final List<String> QUERY_COMMANDS = Arrays.asList(
		"SELECT",
		"SHOW_MODULES",
		"SHOW_CATALOGS",
		"SHOW_CURRENT_CATALOG",
		"SHOW_DATABASES",
		"SHOW_CURRENT_DATABASE",
		"SHOW_TABLES",
		"SHOW_FUNCTIONS",
		"DESCRIBE",
		"EXPLAIN");

	public static final List<String> DDL_COMMANDS = Arrays.asList(
		"CREATE_VIEW",
		"DROP_VIEW",
		"CREATE_TABLE",
		"DROP_TABLE",
		"ALTER_TABLE",
		"CREATE_DATABASE",
		"DROP_DATABASE",
		"ALTER_DATABASE",
		"SET",
		"RESET",
		"USE_CATALOG",
		"USE");

	public static Pattern sqlPatternToJavaPattern(String sqlPattern) {
		return Pattern.compile(sqlPattern
			.replace("%", ".*")
			.replace("_", ".?"));
	}
}
