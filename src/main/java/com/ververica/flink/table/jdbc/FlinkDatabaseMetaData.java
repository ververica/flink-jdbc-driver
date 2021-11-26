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

import com.ververica.flink.table.gateway.rest.message.GetInfoResponseBody;
import com.ververica.flink.table.gateway.rest.message.StatementExecuteResponseBody;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.TableSchemaUtil;
import com.ververica.flink.table.jdbc.rest.RestUtils;
import com.ververica.flink.table.jdbc.rest.SessionClient;
import com.ververica.flink.table.jdbc.resulthandler.ResultHandlerFactory;
import com.ververica.flink.table.jdbc.type.FlinkSqlType;
import com.ververica.flink.table.jdbc.type.FlinkSqlTypes;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Flink JDBC database meta data.
 */
public class FlinkDatabaseMetaData implements DatabaseMetaData {

	public static final String DRIVER_VERSION = "0.1";
	public static final String JDBC_VERSION = "4.2";

	private static final String[] SUPPORTED_TABLE_TYPES = new String[] {
		"TABLE", "VIEW"
	};

	private GetInfoResponseBody infoResponse;

	private final SessionClient session;
	private final FlinkConnection connection;

	public FlinkDatabaseMetaData(SessionClient session, FlinkConnection connection) {
		this.session = session;
		this.connection = connection;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#allProceduresAreCallable is not supported");
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		String url =  FlinkDriver.URL_PREFIX + session.getServerHost() + ":" + session.getServerPort() +
			"?planner=" + session.getPlanner();
		for (Map.Entry<String, String> entry: session.getProperties().entrySet()) {
			url += "&" + entry.getKey() + "=" + entry.getValue();
		}
		return url;
	}

	@Override
	public String getUserName() throws SQLException {
		return null;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#isReadOnly is not supported");
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#nullsAreSortedHigh is not supported");
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#nullsAreSortedLow is not supported");
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#nullsAreSortedAtStart is not supported");
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#nullsAreSortedAtEnd is not supported");
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return getInfoResponse().getProductName();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return getInfoResponse().getFlinkVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return "Flink Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return DRIVER_VERSION;
	}

	@Override
	public int getDriverMajorVersion() {
		return Integer.valueOf(DRIVER_VERSION.split("\\.")[0]);
	}

	@Override
	public int getDriverMinorVersion() {
		return Integer.valueOf(DRIVER_VERSION.split("\\.")[1]);
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#usesLocalFiles is not supported");
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#usesLocalFilePerTable is not supported");
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsMixedCaseIdentifiers is not supported");
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#storesUpperCaseIdentifiers is not supported");
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#storesLowerCaseIdentifiers is not supported");
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#storesMixedCaseIdentifiers is not supported");
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsMixedCaseQuotedIdentifiers is not supported");
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#storesUpperCaseQuotedIdentifiers is not supported");
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#storesLowerCaseQuotedIdentifiers is not supported");
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#storesMixedCaseQuotedIdentifiers is not supported");
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		// TODO verify this
		return "`";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSQLKeywords is not supported");
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getNumericFunctions is not supported");
	}

	@Override
	public String getStringFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getStringFunctions is not supported");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSystemFunctions is not supported");
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getTimeDateFunctions is not supported");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSearchStringEscape is not supported");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		// TODO
		//  we currently do not support this,
		//  but we can't throw a SQLException because we want to support beeline
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#nullPlusNonNullIsNull is not supported");
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsConvert is not supported");
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsConvert is not supported");
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsMultipleResultSets is not supported");
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsMultipleTransactions is not supported");
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsMinimumSQLGrammar is not supported");
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsCoreSQLGrammar is not supported");
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsExtendedSQLGrammar is not supported");
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsANSI92EntryLevelSQL is not supported");
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsANSI92IntermediateSQL is not supported");
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsANSI92FullSQL is not supported");
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsIntegrityEnhancementFacility is not supported");
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "database";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getProcedureTerm is not supported");
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "catalog";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#isCatalogAtStart is not supported");
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getCatalogSeparator is not supported");
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsSchemasInProcedureCalls is not supported");
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsSchemasInIndexDefinitions is not supported");
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsSchemasInPrivilegeDefinitions is not supported");
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsCatalogsInProcedureCalls is not supported");
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsCatalogsInIndexDefinitions is not supported");
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsCatalogsInPrivilegeDefinitions is not supported");
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsPositionedDelete is not supported");
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsPositionedUpdate is not supported");
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsSelectForUpdate is not supported");
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsStoredProcedures is not supported");
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsOpenCursorsAcrossCommit is not supported");
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsOpenCursorsAcrossRollback is not supported");
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsOpenStatementsAcrossCommit is not supported");
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsOpenStatementsAcrossRollback is not supported");
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxBinaryLiteralLength is not supported");
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxCharLiteralLength is not supported");
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnNameLength is not supported");
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnsInGroupBy is not supported");
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnsInIndex is not supported");
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnsInOrderBy is not supported");
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnsInSelect is not supported");
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxColumnsInTable is not supported");
	}

	@Override
	public int getMaxConnections() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxConnections is not supported");
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxCursorNameLength is not supported");
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxIndexLength is not supported");
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxSchemaNameLength is not supported");
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxProcedureNameLength is not supported");
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxCatalogNameLength is not supported");
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxRowSize is not supported");
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#doesMaxRowSizeIncludeBlobs is not supported");
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxStatementLength is not supported");
	}

	@Override
	public int getMaxStatements() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxStatements is not supported");
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxTableNameLength is not supported");
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxTablesInSelect is not supported");
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getMaxUserNameLength is not supported");
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getDefaultTransactionIsolation is not supported");
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsTransactions is not supported");
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsTransactionIsolationLevel is not supported");
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions is not supported");
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsDataManipulationTransactionsOnly is not supported");
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#dataDefinitionCausesTransactionCommit is not supported");
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#dataDefinitionIgnoredInTransactions is not supported");
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getProcedures is not supported");
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getProcedureColumns is not supported");
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
		if (tableNamePattern == null) {
			tableNamePattern = "%";
		}
		if (types == null) {
			types = SUPPORTED_TABLE_TYPES;
		}

		if ("".equals(catalog) || "".equals(schemaPattern)) {
			// every Flink database belongs to a catalog and a database
			return FlinkResultSet.of(
					com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
							.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
							.columns(new GetTableResultColumnInfos().getColumnInfos())
							.data(Collections.emptyList())
							.build());
		}

		String oldCatalog = connection.getCatalog();
		String oldDatabase = connection.getSchema();

		List<TableResultData> candidates = new ArrayList<>();
		ResultSet databaseResult = getSchemas(catalog, schemaPattern);
		while (databaseResult.next()) {
			appendTablesAndViewsInDatabase(
				databaseResult.getString(2), databaseResult.getString(1), candidates);
		}
		candidates.sort((o1, o2) -> {
			if (o1.type.equals(o2.type)) {
				if (o1.catalog.equals(o2.catalog)) {
					if (o1.database.equals(o2.database)) {
						return o1.table.compareTo(o2.table);
					}
					return o1.database.compareTo(o2.database);
				}
				return o1.catalog.compareTo(o2.catalog);
			}
			return o1.type.compareTo(o2.type);
		});

		connection.setCatalog(oldCatalog);
		connection.setSchema(oldDatabase);

		// match with table name pattern
		List<Row> matches = new ArrayList<>();
		GetTableResultColumnInfos columnInfos = new GetTableResultColumnInfos();
		Pattern javaPattern = FlinkJdbcUtils.sqlPatternToJavaPattern(tableNamePattern);
		List<String> typesList = Arrays.asList(types);
		for (TableResultData candidate : candidates) {
			if (typesList.contains(candidate.type) && javaPattern.matcher(candidate.table).matches()) {
				matches.add(columnInfos.process(candidate));
			}
		}
		return FlinkResultSet.of(
				com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
						.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
						.columns(columnInfos.getColumnInfos())
						.data(matches)
						.build());
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return getSchemas(null, null);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		final String catalogColumn = "TABLE_CAT";

		ResultSet result = getImmediateSingleSqlResultSet("SHOW CATALOGS");

		// we have to recreate a result set to
		// change the column name to the one specified by the java doc
		// and order the result
		List<String> names = new ArrayList<>();
		int maxCatalogNameLength = 1;
		while (result.next()) {
			names.add(result.getString(1));
		}
		names.sort(String::compareTo);

		List<Row> rows = new ArrayList<>();
		for (String name : names) {
			rows.add(Row.of(name));
			maxCatalogNameLength = Math.max(maxCatalogNameLength, name.length());
		}

		return FlinkResultSet.of(
				com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
						.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
						.columns(Collections.singletonList(
								ColumnInfo.create(catalogColumn, new VarCharType(true, maxCatalogNameLength))))
						.data(rows)
						.build());
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		final String tableTypeColumn = "TABLE_TYPE";

		List<Row> rows = new ArrayList<>();
		int maxTypeNameLength = 1;
		for (String type : SUPPORTED_TABLE_TYPES) {
			rows.add(Row.of(type));
			maxTypeNameLength = Math.max(maxTypeNameLength, type.length());
		}

		return FlinkResultSet.of(
				com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
						.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
						.columns(Collections.singletonList(
								ColumnInfo.create(tableTypeColumn, new VarCharType(false, maxTypeNameLength))))
						.data(rows)
						.build());
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		if (columnNamePattern == null) {
			columnNamePattern = "%";
		}

		String oldCatalog = connection.getCatalog();
		String oldDatabase = connection.getSchema();

		List<ColumnResultData> candidates = new ArrayList<>();
		ResultSet tableResult = getTables(catalog, schemaPattern, tableNamePattern, SUPPORTED_TABLE_TYPES);
		while (tableResult.next()) {
			appendColumnsInTable(
				tableResult.getString(1),
				tableResult.getString(2),
				tableResult.getString(3),
				candidates);
		}
		candidates.sort((o1, o2) -> {
			if (o1.catalog.equals(o2.catalog)) {
				if (o1.database.equals(o2.database)) {
					if (o1.table.equals(o2.table)) {
						return o1.pos - o2.pos;
					}
					return o1.table.compareTo(o2.table);
				}
				return o1.database.compareTo(o2.database);
			}
			return o1.catalog.compareTo(o2.catalog);
		});

		connection.setCatalog(oldCatalog);
		connection.setSchema(oldDatabase);

		// match with column name pattern
		List<Row> matches = new ArrayList<>();
		GetColumnResultColumnInfos columnInfos = new GetColumnResultColumnInfos();
		Pattern javaPattern = FlinkJdbcUtils.sqlPatternToJavaPattern(columnNamePattern);
		for (ColumnResultData candidate : candidates) {
			if (javaPattern.matcher(candidate.column).matches()) {
				matches.add(columnInfos.process(candidate));
			}
		}
		return FlinkResultSet.of(
				com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
						.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
						.columns(columnInfos.getColumnInfos())
						.data(matches)
						.build());
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getColumnPrivileges is not supported");
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getTablePrivileges is not supported");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getBestRowIdentifier is not supported");
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getVersionColumns is not supported");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		String oldCatalog = connection.getCatalog();
		String oldDatabase = connection.getSchema();

		ResultSet ret;
		List<Row> matches = new ArrayList<>();
		GetPrimaryKeyResultColumnInfos columnInfos = new GetPrimaryKeyResultColumnInfos();

		TableSchema tableSchema = getTableSchema(catalog, schema, table);
		if (tableSchema.getPrimaryKey().isPresent()) {
			List<String> keyNames = tableSchema.getPrimaryKey().get().getColumns();
			for (TableColumn column : tableSchema.getTableColumns()) {
				int pkIdx = keyNames.indexOf(column.getName());
				if (pkIdx >= 0) {
					matches.add(columnInfos.process(catalog, schema, table, column.getName(), pkIdx));
				}
			}
			ret = FlinkResultSet.of(
					com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
							.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
							.columns(columnInfos.getColumnInfos())
							.data(matches)
							.build());
		} else {
			// no primary keys
			ret = FlinkResultSet.of(
					com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
							.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
							.columns(columnInfos.getColumnInfos())
							.data(Collections.emptyList())
							.build());
		}

		connection.setCatalog(oldCatalog);
		connection.setSchema(oldDatabase);
		return ret;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getImportedKeys is not supported");
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getExportedKeys is not supported");
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getCrossReference is not supported");
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getTypeInfo is not supported");
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getIndexInfo is not supported");
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsResultSetConcurrency is not supported");
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#ownUpdatesAreVisible is not supported");
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#ownDeletesAreVisible is not supported");
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#ownInsertsAreVisible is not supported");
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#othersUpdatesAreVisible is not supported");
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#othersDeletesAreVisible is not supported");
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#othersInsertsAreVisible is not supported");
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#updatesAreDetected is not supported");
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#deletesAreDetected is not supported");
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#insertsAreDetected is not supported");
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsBatchUpdates is not supported");
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getUDTs is not supported");
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsSavepoints is not supported");
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsNamedParameters is not supported");
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsMultipleOpenResults is not supported");
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsGetGeneratedKeys is not supported");
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSuperTypes is not supported");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSuperTypes is not supported");
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getAttributes is not supported");
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsResultSetHoldability is not supported");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getResultSetHoldability is not supported");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return Integer.valueOf(getInfoResponse().getFlinkVersion().split("\\.")[0]);
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return Integer.valueOf(getInfoResponse().getFlinkVersion().split("\\.")[1]);
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return Integer.valueOf(JDBC_VERSION.split("\\.")[0]);
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return Integer.valueOf(JDBC_VERSION.split("\\.")[1]);
	}

	@Override
	public int getSQLStateType() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getSQLStateType is not supported");
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#locatorsUpdateCopy is not supported");
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#supportsStatementPooling is not supported");
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getRowIdLifetime is not supported");
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		if (schemaPattern == null) {
			schemaPattern = "%";
		}

		if ("".equals(catalog)) {
			// every Flink database belongs to a catalog
			return FlinkResultSet.of(
					com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
							.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
							.columns(new GetSchemaColumnInfos().getColumnInfos())
							.data(Collections.emptyList())
							.build());
		}

		List<SchemaResultData> candidates = new ArrayList<>();
		if (catalog == null) {
			String oldCatalog = connection.getCatalog();
			ResultSet catalogResult = getCatalogs();
			while (catalogResult.next()) {
				appendDatabasesInCatalog(catalogResult.getString(1), candidates);
			}
			connection.setCatalog(oldCatalog);
		} else {
			String oldCatalog = connection.getCatalog();
			appendDatabasesInCatalog(catalog, candidates);
			connection.setCatalog(oldCatalog);
		}
		candidates.sort((o1, o2) -> {
			if (o1.catalog.equals(o2.catalog)) {
				return o1.database.compareTo(o2.database);
			}
			return o1.catalog.compareTo(o2.catalog);
		});

		// match with schema pattern
		List<Row> matches = new ArrayList<>();
		GetSchemaColumnInfos columnInfos = new GetSchemaColumnInfos();
		Pattern javaPattern = FlinkJdbcUtils.sqlPatternToJavaPattern(schemaPattern);
		for (SchemaResultData candidate : candidates) {
			if (javaPattern.matcher(candidate.database).matches()) {
				matches.add(columnInfos.process(candidate));
			}
		}
		return FlinkResultSet.of(
				com.ververica.flink.table.gateway.rest.result.ResultSet.builder()
						.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
						.columns(columnInfos.getColumnInfos())
						.data(matches)
						.build());
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#supportsStoredFunctionsUsingCallSyntax is not supported");
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException(
			"FlinkDatabaseMetaData#autoCommitFailureClosesAllResultSets is not supported");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getClientInfoProperties is not supported");
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getFunctions is not supported");
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getFunctionColumns is not supported");
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#getPseudoColumns is not supported");
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#generatedKeyAlwaysReturned is not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#unwrap is not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("FlinkDatabaseMetaData#isWrapperFor is not supported");
	}

	private StatementExecuteResponseBody getImmediateSingleResponse(String stmt)
			throws SQLException {
		StatementExecuteResponseBody response;
		response = session.submitStatement(stmt);
		Preconditions.checkState(
			response.getResults().size() == 1,
			stmt + " should return exactly 1 result set. This is a bug.");
		Either<JobID, com.ververica.flink.table.gateway.rest.result.ResultSet> jobIdOrResultSet =
			RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0));
		Preconditions.checkState(
			jobIdOrResultSet.isRight(),
			stmt + " should directly return the result. This is a bug.");
		return response;
	}

	private ResultSet getImmediateSingleSqlResultSet(String stmt) throws SQLException {
		StatementExecuteResponseBody response = getImmediateSingleResponse(stmt);
		return new FlinkResultSet(
			session,
			RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0)),
			ResultHandlerFactory.getResultHandlerByStatementType(response.getStatementTypes().get(0)),
			0,
			null);
	}

	private void appendDatabasesInCatalog(
			String catalog,
			List<SchemaResultData> candidates) throws SQLException {
		connection.setCatalog(catalog);
		ResultSet result = getImmediateSingleSqlResultSet("SHOW DATABASES");
		while (result.next()) {
			candidates.add(new SchemaResultData(catalog, result.getString(1)));
		}
	}

	private void appendTablesAndViewsInDatabase(
			String catalog,
			String database,
			List<TableResultData> candidates) throws SQLException {
		connection.setCatalog(catalog);
		connection.setSchema(database);
		ResultSet tables = getImmediateSingleSqlResultSet("SHOW TABLES");
		while (tables.next()) {
			candidates.add(new TableResultData(catalog, database, tables.getString(1), "TABLE"));
		}
		ResultSet views = getImmediateSingleSqlResultSet("SHOW VIEWS");
		while (views.next()) {
			candidates.add(new TableResultData(catalog, database, views.getString(1), "VIEW"));
		}
	}

	private TableSchema getTableSchema(String catalog, String database, String table) throws SQLException {
		connection.setCatalog(catalog);
		connection.setSchema(database);
		StatementExecuteResponseBody response = getImmediateSingleResponse("DESCRIBE " + table);
		// we use raw results here to get table schema
		ResultSet result = new FlinkResultSet(
			session,
			RestUtils.getEitherJobIdOrResultSet(response.getResults().get(0)),
			ResultHandlerFactory.getDefaultResultHandler(),
			0,
			null);

		boolean hasNext = result.next();
		Preconditions.checkState(
			hasNext,
			"DESCRIBE statement must return exactly " +
				"one serialized table schema json string. This is a bug.");
		try {
			return TableSchemaUtil.readTableSchemaFromJson(result.getString(1));
		} catch (JsonProcessingException e) {
			throw new SQLException("Failed to parse json to table schema", e);
		}
	}

	private void appendColumnsInTable(
			String catalog,
			String database,
			String table,
			List<ColumnResultData> candidates) throws SQLException {
		TableSchema tableSchema = getTableSchema(catalog, database, table);
		int idx = 0;
		for (TableColumn column : tableSchema.getTableColumns()) {
			candidates.add(new ColumnResultData(
				catalog, database, table, column.getName(), ++idx, column.getType().getLogicalType()));
		}
	}

	private GetInfoResponseBody getInfoResponse() throws SQLException {
		if (infoResponse == null) {
			infoResponse = session.getInfo();
		}
		return infoResponse;
	}

	/**
	 * Candidate result for schema related interface.
	 */
	private static class SchemaResultData {
		final String catalog;
		final String database;

		SchemaResultData(String catalog, String database) {
			this.catalog = catalog;
			this.database = database;
		}
	}

	/**
	 * Helper class to generate {@link FlinkResultSet}
	 * for FlinkDatabaseMetaData#getSchemas interface.
	 */
	private static class GetSchemaColumnInfos {
		private int maxCatalogNameLength = 1;
		private int maxDatabaseNameLength = 1;

		Row process(SchemaResultData data) {
			maxCatalogNameLength = Math.max(maxCatalogNameLength, data.catalog.length());
			maxDatabaseNameLength = Math.max(maxDatabaseNameLength, data.database.length());
			return Row.of(data.database, data.catalog);
		}

		List<ColumnInfo> getColumnInfos() {
			// according to the java doc of DatabaseMetaData#getSchemas
			return Arrays.asList(
				ColumnInfo.create("TABLE_SCHEM", new VarCharType(false, maxDatabaseNameLength)),
				ColumnInfo.create("TABLE_CATALOG", new VarCharType(true, maxCatalogNameLength)));
		}
	}

	/**
	 * Candidate result for table related interface.
	 */
	private static class TableResultData {
		final String catalog;
		final String database;
		final String table;
		final String type;

		TableResultData(String catalog, String database, String table, String type) {
			this.catalog = catalog;
			this.database = database;
			this.table = table;
			this.type = type;
		}
	}

	/**
	 * Helper class to generate {@link FlinkResultSet}
	 * for FlinkDatabaseMetaData#getTables interface.
	 */
	private static class GetTableResultColumnInfos {
		private int maxCatalogNameLength = 1;
		private int maxDatabaseNameLength = 1;
		private int maxTableNameLength = 1;

		Row process(TableResultData data) {
			maxCatalogNameLength = Math.max(maxCatalogNameLength, data.catalog.length());
			maxDatabaseNameLength = Math.max(maxDatabaseNameLength, data.database.length());
			maxTableNameLength = Math.max(maxTableNameLength, data.table.length());
			return Row.of(data.catalog, data.database, data.table, data.type, null, null, null, null, null, null);
		}

		List<ColumnInfo> getColumnInfos() {
			// according to the java doc of DatabaseMetaData#getTables
			return Arrays.asList(
				ColumnInfo.create("TABLE_CAT", new VarCharType(true, maxCatalogNameLength)),
				ColumnInfo.create("TABLE_SCHEM", new VarCharType(true, maxDatabaseNameLength)),
				ColumnInfo.create("TABLE_NAME", new VarCharType(false, maxTableNameLength)),
				// currently can only be TABLE or VIEW
				ColumnInfo.create("TABLE_TYPE", new VarCharType(false, 5)),
				// currently these columns are null
				ColumnInfo.create("REMARKS", new VarCharType(true, 1)),
				ColumnInfo.create("TYPE_CAT", new VarCharType(true, 1)),
				ColumnInfo.create("TYPE_SCHEM", new VarCharType(true, 1)),
				ColumnInfo.create("TYPE_NAME", new VarCharType(true, 1)),
				ColumnInfo.create("SELF_REFERENCING_COL_NAME", new VarCharType(true, 1)),
				ColumnInfo.create("REF_GENERATION", new VarCharType(true, 1)));
		}
	}

	/**
	 * Candidate result for column related interface.
	 */
	private static class ColumnResultData {
		final String catalog;
		final String database;
		final String table;
		final String column;
		final int pos;
		final LogicalType logicalType;
		final FlinkSqlType sqlType;

		ColumnResultData(
				String catalog,
				String database,
				String table,
				String column,
				int pos,
				LogicalType logicalType) {
			this.catalog = catalog;
			this.database = database;
			this.table = table;
			this.column = column;
			this.pos = pos;
			this.logicalType = logicalType;
			this.sqlType = FlinkSqlTypes.getType(logicalType);
		}
	}

	/**
	 * Helper class to generate {@link FlinkResultSet}
	 * for FlinkDatabaseMetaData#getColumns interface.
	 */
	private static class GetColumnResultColumnInfos {
		private int maxCatalogNameLength = 1;
		private int maxDatabaseNameLength = 1;
		private int maxTableNameLength = 1;
		private int maxColumnNameLength = 1;
		private int maxTypeNameLength = 1;

		Row process(ColumnResultData data) {
			maxCatalogNameLength = Math.max(maxCatalogNameLength, data.catalog.length());
			maxDatabaseNameLength = Math.max(maxDatabaseNameLength, data.database.length());
			maxTableNameLength = Math.max(maxTableNameLength, data.table.length());
			maxColumnNameLength = Math.max(maxColumnNameLength, data.column.length());
			maxTypeNameLength = Math.max(maxTypeNameLength, data.logicalType.toString().length());

			boolean isNumeric = FlinkSqlTypes.isNumeric(data.sqlType);
			boolean isChar = FlinkSqlTypes.isChar(data.sqlType);

			return Row.of(
				data.catalog, // TABLE_CAT
				data.database, // TABLE_SCHEM
				data.table, // TABLE_NAME
				data.column, // COLUMN_NAME
				data.sqlType.getSqlType(), // DATA_TYPE
				data.logicalType.toString(), // TYPE_NAME
				data.sqlType.getPrecision(), // COLUMN_SIZE
				null, // BUFFER_LENGTH unused
				isNumeric ? data.sqlType.getSqlType() : null, // DECIMAL_DIGITS
				isNumeric ? 10 : null, // NUM_PREC_RADIX
				data.logicalType.isNullable() ? columnNullable : columnNoNulls, // NULLABLE
				null, // REMARKS
				null, // COLUMN_DEF
				null, // SQL_DATA_TYPE unused
				null, // SQL_DATETIME_SUB unused
				isChar ? data.sqlType.getPrecision() : null, // CHAR_OCTET_LENGTH
				data.pos, // ORDINAL_POSITION
				data.logicalType.isNullable() ? "YES" : "NO", // IS_NULLABLE
				null, // SCOPE_CATALOG
				null, // SCOPE_SCHEMA
				null, // SCOPE_TABLE
				null, // SOURCE_DATA_TYPE
				"", // IS_AUTOINCREMENT
				"" // IS_GENERATEDCOLUMN
			);
		}

		public List<ColumnInfo> getColumnInfos() {
			// according to the java doc of DatabaseMetaData#getColumns
			return Arrays.asList(
				ColumnInfo.create("TABLE_CAT", new VarCharType(true, maxCatalogNameLength)),
				ColumnInfo.create("TABLE_SCHEM", new VarCharType(true, maxDatabaseNameLength)),
				ColumnInfo.create("TABLE_NAME", new VarCharType(false, maxTableNameLength)),
				ColumnInfo.create("COLUMN_NAME", new VarCharType(false, maxColumnNameLength)),
				ColumnInfo.create("DATA_TYPE", new IntType(false)),
				ColumnInfo.create("TYPE_NAME", new VarCharType(false, maxTypeNameLength)),
				ColumnInfo.create("COLUMN_SIZE", new IntType(false)),
				ColumnInfo.create("BUFFER_LENGTH", new IntType(true)),
				ColumnInfo.create("DECIMAL_DIGITS", new IntType(true)),
				ColumnInfo.create("NUM_PREC_RADIX", new IntType(true)),
				ColumnInfo.create("NULLABLE", new IntType(false)),
				ColumnInfo.create("REMARKS", new VarCharType(true, 1)),
				ColumnInfo.create("COLUMN_DEF", new VarCharType(true, 1)),
				ColumnInfo.create("SQL_DATA_TYPE", new IntType(true)),
				ColumnInfo.create("SQL_DATETIME_SUB", new IntType(true)),
				ColumnInfo.create("CHAR_OCTET_LENGTH", new IntType(true)),
				ColumnInfo.create("ORDINAL_POSITION", new IntType(false)),
				ColumnInfo.create("IS_NULLABLE", new VarCharType(false, 3)), // YES or NO
				ColumnInfo.create("SCOPE_CATALOG", new VarCharType(true, 1)),
				ColumnInfo.create("SCOPE_SCHEMA", new VarCharType(true, 1)),
				ColumnInfo.create("SCOPE_TABLE", new VarCharType(true, 1)),
				ColumnInfo.create("SOURCE_DATA_TYPE", new SmallIntType(true)),
				ColumnInfo.create("IS_AUTOINCREMENT", new VarCharType(false, 1)), // empty string
				ColumnInfo.create("IS_GENERATEDCOLUMN", new VarCharType(false, 1)) // empty string
			);
		}
	}

	/**
	 * Helper class to generate {@link FlinkResultSet}
	 * for FlinkDatabaseMetaData#getPrimaryKeys interface.
	 */
	private static class GetPrimaryKeyResultColumnInfos {
		private int maxCatalogNameLength = 1;
		private int maxDatabaseNameLength = 1;
		private int maxTableNameLength = 1;
		private int maxColumnNameLength = 1;

		Row process(String catalog, String database, String table, String column, int pkSeq) {
			maxCatalogNameLength = Math.max(maxCatalogNameLength, catalog.length());
			maxDatabaseNameLength = Math.max(maxDatabaseNameLength, database.length());
			maxTableNameLength = Math.max(maxTableNameLength, table.length());
			maxColumnNameLength = Math.max(maxColumnNameLength, column.length());
			return Row.of(catalog, database, table, column, pkSeq, null);
		}

		public List<ColumnInfo> getColumnInfos() {
			// according to the java doc of DatabaseMetaData#getPrimaryKeys
			return Arrays.asList(
				ColumnInfo.create("TABLE_CAT", new VarCharType(true, maxCatalogNameLength)),
				ColumnInfo.create("TABLE_SCHEM", new VarCharType(true, maxDatabaseNameLength)),
				ColumnInfo.create("TABLE_NAME", new VarCharType(false, maxTableNameLength)),
				ColumnInfo.create("COLUMN_NAME", new VarCharType(false, maxColumnNameLength)),
				ColumnInfo.create("KEY_SEQ", new SmallIntType(false)),
				ColumnInfo.create("PK_NAME", new VarCharType(true, 1)));
		}
	}
}
