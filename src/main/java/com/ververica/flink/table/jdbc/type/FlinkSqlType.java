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

package com.ververica.flink.table.jdbc.type;

/**
 * A data structure which records the information JDBC needed for SQL types.
 */
public class FlinkSqlType {

	private final int sqlType;
	private final int precision;
	private final int scale;
	private final int displaySize;

	FlinkSqlType(int sqlType, int precision, int scale, int displaySize) {
		this.sqlType = sqlType;
		this.precision = precision;
		this.scale = scale;
		this.displaySize = displaySize;
	}

	public int getSqlType() {
		return sqlType;
	}

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public int getDisplaySize() {
		return displaySize;
	}
}
