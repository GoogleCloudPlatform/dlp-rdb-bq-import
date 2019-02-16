/* Copyright 2018 Google LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.google.swarm.sqlserver.migration.common;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.FieldId;

public class ServerUtil {

	public static final Logger LOG = LoggerFactory.getLogger(ServerUtil.class);
	private static final String QUERY_TABLES = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES";
	private static final String QUERY_COLUMNS = "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? ORDER BY ORDINAL_POSITION ASC";
	private static final String QUERY_PRIMARY_KEY = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME = ? AND constraint_name LIKE 'PK%'";

	public static final Map<SqlDataType, String> msSqlToBqTypeMap = ImmutableMap.<SqlDataType, String>builder()
			.put(SqlDataType.VARCHAR, "STRING").put(SqlDataType.NVARCHAR, "STRING").put(SqlDataType.CHAR, "STRING")
			.put(SqlDataType.NCHAR, "STRING").put(SqlDataType.TEXT, "STRING").put(SqlDataType.NTEXT, "STRING")
			.put(SqlDataType.BIGINT, "INTEGER").put(SqlDataType.INT, "INTEGER").put(SqlDataType.TINYINT, "INTEGER")
			.put(SqlDataType.SMALLINT, "INTEGER").put(SqlDataType.NUMERIC, "FLOAT").put(SqlDataType.DECIMAL, "FLOAT")
			.put(SqlDataType.MONEY, "FLOAT").put(SqlDataType.SMALLMONEY, "FLOAT").put(SqlDataType.FLOAT, "FLOAT")
			.put(SqlDataType.REAL, "FLOAT").put(SqlDataType.BIT, "BOOLEAN").put(SqlDataType.DATE, "DATE")
			.put(SqlDataType.TIME, "TIME").put(SqlDataType.DATETIME, "DATETIME").put(SqlDataType.DATETIME2, "DATETIME")
			.put(SqlDataType.DATETIMEOFFSET, "TIMESTAMP").put(SqlDataType.SMALLDATETIME, "DATETIME")
			.put(SqlDataType.TIMESTAMP, "STRING").put(SqlDataType.BINARY, "BYTES").put(SqlDataType.IMAGE, "BYTES")
			.put(SqlDataType.VARBINARY, "BYTES").put(SqlDataType.UNIQUEIDENTIFIER, "STRING").build();

	public static Connection getConnection(String connectionUrl) throws SQLException {

		Connection connection = null;

		try {
			/*
			 * Currently from any JDBC 4.0 drivers that are found in the class path are
			 * automatically loaded. So, there is no need for Class.forName(). SQL Server
			 * JDBC and JTDS Drivers are par of build.gradle at this time and included in
			 * runtime. Please add required drivers if you need support for additional
			 * databases.
			 */
			connection = DriverManager.getConnection(connectionUrl);
			LOG.debug("Connection Status: " + connection.isClosed());

		} catch (Exception e) {
			LOG.error("***ERROR*** Unable to create connection {}", e.toString());
			throw new RuntimeException(e);
		}

		return connection;

	}

	public static List<SqlTable> getTablesList(Connection connection, List<DLPProperties> dlpConfigList) {
		List<SqlTable> tables = new ArrayList<SqlTable>();
		long key = 1;

		try {

			if (connection != null) {
				Statement statement = connection.createStatement();

				ResultSet rs = statement.executeQuery(QUERY_TABLES);
				while (rs.next()) {
					SqlTable table = new SqlTable(key);
					table.setSchema(rs.getString("TABLE_SCHEMA"));
					table.setName(rs.getString("TABLE_NAME"));
					table.setDlpConfig(ServerUtil.extractDLPConfig(table.getName(), dlpConfigList));
					table.setType(rs.getString("TABLE_TYPE"));
					tables.add(table);
					key = key + 1;

				}
			}
		} catch (SQLException e) {
			LOG.error("***ERROR*** {} Unable to get list of tables {}", e.toString(), QUERY_TABLES);
			throw new RuntimeException(e);
		}
		return tables;
	}

	public static List<SqlTable> getTablesList(Connection connection, String excludedTables,
			List<DLPProperties> dlpConfigList) throws SQLException {
		List<SqlTable> tables = ServerUtil.getTablesList(connection, dlpConfigList);
		String[] excludedTableList = ServerUtil.parseExcludedTables(excludedTables);
		tables.removeIf(table -> Arrays.asList(excludedTableList).contains(table.getName()));
		LOG.debug("Excluded Table size: {}", tables.size());
		return tables;
	}

	public static int getRowCount(Connection connection, String schemaName, String tableName) throws SQLException {

		int count = 0;

		String query = String.format("SELECT COUNT(*) as NUMBER_OF_ROWS FROM %s", tableName);
		LOG.debug("query: {}", query);
		try {

			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {

				count = rs.getInt("NUMBER_OF_ROWS");
			}

		} catch (SQLException e) {
			LOG.error("***ERROR*** {} Unable to get row count for query {}", e.toString(), query);
			throw new RuntimeException(e);
		}
		return count;

	}

	public static String getPrimaryColumn(Connection connection, String tableName) throws SQLException {

		String primaryColumnName = null;

		try {
			PreparedStatement statement = connection.prepareStatement(QUERY_PRIMARY_KEY);
			statement.setString(1, tableName);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {

				primaryColumnName = rs.getString("COLUMN_NAME");
			}

		}

		catch (SQLException e) {
			LOG.error("***ERROR*** {} Unable to get promary column connection {}", e.toString(), QUERY_PRIMARY_KEY);
			throw new RuntimeException(e);
		}
		return primaryColumnName;
	}

	public static List<SqlColumn> getColumnsList(Connection connection, String tableName) throws SQLException {
		List<SqlColumn> columns = new ArrayList<SqlColumn>();
		String primaryKey = ServerUtil.getPrimaryColumn(connection, tableName);
		try {
			PreparedStatement statement = connection.prepareStatement(QUERY_COLUMNS);
			statement.setString(1, tableName);

			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				SqlColumn column = new SqlColumn();
				column.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
				column.setName(rs.getString("COLUMN_NAME"));
				if (column.getName().equalsIgnoreCase(primaryKey)) {
					column.setPrimaryKey(true);
				} else {
					column.setPrimaryKey(false);
				}
				column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
				column.setNullable(rs.getBoolean("IS_NULLABLE"));
				column.setDataType(rs.getString("DATA_TYPE"));
				columns.add(column);

			}
		} catch (SQLException e) {
			LOG.error("***ERROR*** {} Unable to get column list for table {} , Query {}", e.toString(), tableName,
					QUERY_COLUMNS);
			throw new RuntimeException(e);
		}

		return columns;
	}

	public static TableSchema getBigQuerySchema(List<SqlColumn> tableColumns) throws SQLException {
		if (tableColumns == null)
			return null;

		TableSchema schema = new TableSchema();
		List<TableFieldSchema> fieldSchemas = new ArrayList<TableFieldSchema>();
		for (SqlColumn column : tableColumns) {
			TableFieldSchema fieldSchema = new TableFieldSchema();
			fieldSchema.setName(column.getName());
			String dataTypeString = column.getDataType().toUpperCase();
			SqlDataType dataType;
			try {
				dataType = SqlDataType.valueOf(dataTypeString);
			} catch (Exception e) {
				LOG.error(String.format(" ***ERROR*** Unrecognized data type %s", dataTypeString));
				throw new SQLException(String.format("Unrecognized data type %s", dataTypeString));
			}
			if (ServerUtil.msSqlToBqTypeMap.containsKey(dataType)) {
				fieldSchema.setType(ServerUtil.msSqlToBqTypeMap.get(dataType));
			} else {
				LOG.error(String.format("***ERROR*** DataType %s not supported!", dataType));
				throw new SQLException(String.format("DataType %s not supported!", dataType));
			}
			fieldSchemas.add(fieldSchema);
		}
		schema.setFields(fieldSchemas);

		return schema;
	}

	public static String findPrimaryKey(List<SqlColumn> columnNames) {

		SqlColumn columnName = columnNames.stream().filter(column -> column.getPrimaryKey()).findFirst().orElse(null);
		LOG.debug("PRIMARY KEY {}", columnName.getName());
		return columnName.getName();

	}

	public static String[] parseExcludedTables(String excludedTables) {
		return excludedTables.split("-");

	}

	public static List<DLPProperties> parseDLPconfig(ValueProvider<String> bucket, ValueProvider<String> blob) {

		DLPProperties[] configMaps = null;
		if ((!bucket.isAccessible() || !blob.isAccessible())) {
			return null;
		}
		if (bucket.get() != null && blob.get() != null) {

			Storage storage = StorageOptions.getDefaultInstance().getService();
			BlobId blobId = BlobId.of(bucket.get(), blob.get());
			byte[] bytes = storage.readAllBytes(blobId);
			configMaps = new Gson().fromJson(new String(bytes, StandardCharsets.UTF_8), DLPProperties[].class);

		}

		List<DLPProperties> dlpConfigs = Arrays.asList(Optional.ofNullable(configMaps).orElse(new DLPProperties[0]));

		return dlpConfigs;

	}

	public static DLPProperties extractDLPConfig(String tableId, List<DLPProperties> dlpConfigList) {

		DLPProperties prop = null;
		if (dlpConfigList != null) {
			prop = dlpConfigList.stream().filter(config -> config.getTableName().equals(tableId)).findFirst()
					.orElse(null);
		}

		return prop;
	}

	public static List<FieldId> getHeaders(List<SqlColumn> tableHeaders) {

		String[] columnNames = new String[tableHeaders.size()];

		for (int i = 0; i < columnNames.length; i++) {
			columnNames[i] = tableHeaders.get(i).getName();

		}
		List<FieldId> headers = Arrays.stream(columnNames).map(header -> FieldId.newBuilder().setName(header).build())
				.collect(Collectors.toList());
		LOG.debug("HEADERS SIZE {}", headers.size());
		return headers;
	}

}
