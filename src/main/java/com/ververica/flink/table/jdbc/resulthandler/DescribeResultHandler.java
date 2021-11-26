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
import com.ververica.flink.table.gateway.rest.result.TableSchemaUtil;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A result handler that change the {@link ResultSet} produced by the describe statement of REST API
 * to a form that can be printed to screen.
 */
public class DescribeResultHandler implements ResultHandler {

	@Override
	public ResultSet handleResult(ResultSet raw) {
		List<ColumnInfo> rawColumnInfos = raw.getColumns();
		Preconditions.checkArgument(
			rawColumnInfos.size() == 1 &&
				rawColumnInfos.get(0).getName().equals(ConstantNames.SCHEMA) &&
				rawColumnInfos.get(0).getLogicalType() instanceof VarCharType,
			"Invalid DESCRIBE result schema");
		Preconditions.checkArgument(
			raw.getData().size() == 1,
			"DESCRIBE result should contain exactly 1 json string record");

		List<ColumnInfo> newColumnInfos = Arrays.asList(
			new ColumnInfo("column_name", rawColumnInfos.get(0).getType()),
			new ColumnInfo("column_type", rawColumnInfos.get(0).getType()),
			ColumnInfo.create("nullable", new BooleanType(false)),
			ColumnInfo.create("primary_key", new BooleanType(false)));

		Row rawRow = raw.getData().get(0);
		String json = rawRow.getField(0).toString();
		TableSchema schema;
		try {
			schema = TableSchemaUtil.readTableSchemaFromJson(json);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to parse json to table schema", e);
		}
		List<String> primaryKeys;
		if (schema.getPrimaryKey().isPresent()) {
			primaryKeys = schema.getPrimaryKey().get().getColumns();
		} else {
			primaryKeys = Collections.emptyList();
		}

		List<Row> newRows = new ArrayList<>();
		for (TableColumn column : schema.getTableColumns()) {
			String name = column.getName();
			LogicalType type = column.getType().getLogicalType();
			newRows.add(Row.of(name, type.toString(), type.isNullable(), primaryKeys.contains(name)));
		}

		return ResultSet.builder()
				.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
				.columns(newColumnInfos)
				.data(newRows)
				.build();
	}
}
