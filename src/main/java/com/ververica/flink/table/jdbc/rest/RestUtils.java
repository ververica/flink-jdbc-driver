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

package com.ververica.flink.table.jdbc.rest;

import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;

/**
 * Utility class to handle REST data structures.
 */
public class RestUtils {

	public static JobID getJobID(ResultSet resultSet) {
		if (resultSet.getColumns().size() != 1) {
			throw new IllegalArgumentException("Should contain only one column. This is a bug.");
		}
		if (resultSet.getColumns().get(0).getName().equals(ConstantNames.JOB_ID)) {
			String jobId = (String) resultSet.getData().get(0).getField(0);
			return JobID.fromHexString(jobId);
		} else {
			throw new IllegalArgumentException("Column name should be " + ConstantNames.JOB_ID + ". This is a bug.");
		}
	}

	public static Either<JobID, ResultSet> getEitherJobIdOrResultSet(ResultSet resultSet) {
		if (resultSet.getColumns().size() == 1 && resultSet.getColumns().get(0).getName()
			.equals(ConstantNames.JOB_ID)) {
			String jobId = (String) resultSet.getData().get(0).getField(0);
			return Either.Left(JobID.fromHexString(jobId));
		} else {
			return Either.Right(resultSet);
		}
	}
}
