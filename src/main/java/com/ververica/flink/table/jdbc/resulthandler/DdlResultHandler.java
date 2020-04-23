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

package com.ververica.flink.table.jdbc.resulthandler;

import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.SQLException;
import java.util.List;

/**
 * A result handler that change the {@link ResultSet} produced by the describe statement of REST API
 * to a form that can be printed to screen.
 */
public class DdlResultHandler implements ResultHandler {

	@Override
	public ResultSet handleResult(ResultSet raw) throws SQLException {
		List<ColumnInfo> rawColumnInfos = raw.getColumns();
		Preconditions.checkArgument(
			rawColumnInfos.size() == 1 &&
				rawColumnInfos.get(0).getName().equals(ConstantNames.RESULT) &&
				rawColumnInfos.get(0).getLogicalType() instanceof VarCharType,
			"Invalid DDL result set. This is a bug.");
		Preconditions.checkArgument(
			raw.getData().size() == 1,
			"DDL result should contain exactly 1 string record. This is a bug.");

		Row rawRow = raw.getData().get(0);
		String str = rawRow.getField(0).toString();
		Preconditions.checkArgument(
			ConstantNames.OK.equals(str),
			"DDL result must be " + ConstantNames.OK + ". This is a bug.");

		return ResultSet.builder()
			.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
			.columns(ColumnInfo.create(ConstantNames.AFFECTED_ROW_COUNT, new IntType(false)))
			// according to JDBC Java doc DDL's update count is 0
			.data(Row.of(0))
			.build();
	}
}
