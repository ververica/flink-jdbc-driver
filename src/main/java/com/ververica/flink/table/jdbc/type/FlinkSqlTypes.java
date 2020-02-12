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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.sql.Types;

/**
 * Defines all SQL types with information needed for JDBC.
 */
public class FlinkSqlTypes {

	public static final FlinkSqlType BOOLEAN = new FlinkSqlType(Types.BOOLEAN, 1, 0, 5);
	public static final FlinkSqlType TINYINT = new FlinkSqlType(Types.TINYINT, 3, 0, 4);
	public static final FlinkSqlType SMALLINT = new FlinkSqlType(Types.SMALLINT, 5, 0, 6);
	public static final FlinkSqlType INT = new FlinkSqlType(Types.INTEGER, 10, 0, 11);
	public static final FlinkSqlType BIGINT = new FlinkSqlType(Types.BIGINT, 19, 0, 20);
	public static final FlinkSqlType FLOAT = new FlinkSqlType(Types.FLOAT, 7, 7, 24);
	public static final FlinkSqlType DOUBLE = new FlinkSqlType(Types.DOUBLE, 15, 15, 25);
	public static final FlinkSqlType DATE = new FlinkSqlType(Types.DATE, 10, 0, 10);
	public static final FlinkSqlType NULL = new FlinkSqlType(Types.NULL, 0, 0, 4);
	public static final FlinkSqlType ARRAY = new FlinkSqlType(Types.ARRAY, Integer.MAX_VALUE, 0, Integer.MAX_VALUE);
	public static final FlinkSqlType STRUCT = new FlinkSqlType(Types.STRUCT, Integer.MAX_VALUE, 0, Integer.MAX_VALUE);
	public static final FlinkSqlType OTHER = new FlinkSqlType(Types.OTHER, Integer.MAX_VALUE, 0, Integer.MAX_VALUE);

	public static FlinkSqlType createDecimalType(DecimalType type) {
		int precision = type.getPrecision();
		int scale = type.getScale();
		return new FlinkSqlType(Types.DECIMAL, precision, scale, precision);
	}

	public static FlinkSqlType createCharType(CharType type) {
		int length = type.getLength();
		return new FlinkSqlType(Types.CHAR, length, 0, length);
	}

	public static FlinkSqlType createVarCharType(VarCharType type) {
		int length = type.getLength();
		return new FlinkSqlType(Types.VARCHAR, length, 0, length);
	}

	public static FlinkSqlType createTimeType(TimeType type) {
		int scale = type.getPrecision();
		if (scale > 0) {
			return new FlinkSqlType(Types.TIME, 9 + scale, scale, 9 + scale);
		} else {
			return new FlinkSqlType(Types.TIME, 8, 0, 8);
		}
	}

	public static FlinkSqlType createTimestampType(TimestampType type) {
		int scale = type.getPrecision();
		if (scale > 0) {
			return new FlinkSqlType(Types.TIMESTAMP, 20 + scale, scale, 20 + scale);
		} else {
			return new FlinkSqlType(Types.TIMESTAMP, 19, 0, 19);
		}
	}

	public static FlinkSqlType createZonedTimstampType(ZonedTimestampType type) {
		int scale = type.getPrecision();
		if (scale > 0) {
			return new FlinkSqlType(Types.TIMESTAMP_WITH_TIMEZONE, 27 + scale, scale, 27 + scale);
		} else {
			return new FlinkSqlType(Types.TIMESTAMP_WITH_TIMEZONE, 26, 0, 26);
		}
	}

	public static FlinkSqlType createBinaryType(BinaryType type) {
		int length = type.getLength();
		return new FlinkSqlType(Types.BINARY, length, 0, length);
	}

	public static FlinkSqlType createVarBinaryType(VarBinaryType type) {
		int length = type.getLength();
		return new FlinkSqlType(Types.VARBINARY, length, 0, length);
	}

	public static boolean isNumeric(FlinkSqlType type) {
		switch (type.getSqlType()) {
			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.DECIMAL:
				return true;
			default:
				return false;
		}
	}

	public static boolean isChar(FlinkSqlType type) {
		switch (type.getSqlType()) {
			case Types.CHAR:
			case Types.VARCHAR:
				return true;
			default:
				return false;
		}
	}

	public static FlinkSqlType getType(LogicalType type) {
		if (type instanceof BooleanType) {
			return BOOLEAN;
		} else if (type instanceof TinyIntType) {
			return TINYINT;
		} else if (type instanceof SmallIntType) {
			return SMALLINT;
		} else if (type instanceof IntType) {
			return INT;
		} else if (type instanceof BigIntType) {
			return BIGINT;
		} else if (type instanceof FloatType) {
			return FLOAT;
		} else if (type instanceof DoubleType) {
			return DOUBLE;
		} else if (type instanceof DecimalType) {
			return createDecimalType((DecimalType) type);
		} else if (type instanceof CharType) {
			return createCharType((CharType) type);
		} else if (type instanceof VarCharType) {
			return createVarCharType((VarCharType) type);
		} else if (type instanceof DateType) {
			return DATE;
		} else if (type instanceof TimeType) {
			return createTimeType((TimeType) type);
		} else if (type instanceof TimestampType) {
			return createTimestampType((TimestampType) type);
		} else if (type instanceof BinaryType) {
			return createBinaryType((BinaryType) type);
		} else if (type instanceof VarBinaryType) {
			return createVarBinaryType((VarBinaryType) type);
		} else if (type instanceof NullType) {
			return NULL;
		} else if (type instanceof StructuredType) {
			return STRUCT;
		} else if (type instanceof ArrayType) {
			return ARRAY;
		} else if (type instanceof ZonedTimestampType) {
			return createZonedTimstampType((ZonedTimestampType) type);
		} else {
			return OTHER;
		}
	}
}
