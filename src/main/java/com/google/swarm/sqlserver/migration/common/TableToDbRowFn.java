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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class TableToDbRowFn extends DoFn<SqlTable, KV<SqlTable, List<DbRow>>> {

	private static final Logger LOG = LoggerFactory.getLogger(TableToDbRowFn.class);

	private ValueProvider<String> connectionString;
	private int batchSize;
	private Connection connection;
	private int tableCount;
	private ValueProvider<Integer> offsetCount;

	public TableToDbRowFn(ValueProvider<String> connectionString, ValueProvider<Integer> offsetCount) {
		this.connectionString = connectionString;

		this.batchSize = 0;
		this.connection = null;
		this.tableCount = 0;
		this.offsetCount = offsetCount;

	}

	@ProcessElement
	public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) throws SQLException {

		SqlTable msSqlTable = c.element();
		List<SqlColumn> columnNames = msSqlTable.getCloumnList();
		Statement statement = null;
		List<DbRow> rows = new ArrayList<>();

		for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {

			LOG.info("Started Restriction From: {}, To: {} ", tracker.currentRestriction().getFrom(),
					tracker.currentRestriction().getTo());
			if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
				try {

					String primaryKey = ServerUtil.findPrimaryKey(columnNames);
					statement = connection.createStatement();
					String query = String.format(
							"SELECT * FROM %s.%s ORDER BY %s OFFSET %d * (%d - 1) " + "ROWS FETCH NEXT %d ROWS ONLY",
							msSqlTable.getSchema(), msSqlTable.getName(), primaryKey, this.offsetCount.get(), i,
							this.offsetCount.get());
					LOG.debug("Executing query: " + query);

					statement.executeQuery(query);
					ResultSet rs = statement.executeQuery(query);

					ResultSetMetaData meta = rs.getMetaData();
					int columnCount = meta.getColumnCount();
					while (rs.next()) {
						List<Object> values = new ArrayList<Object>();
						for (int colNumber = 1; colNumber <= columnCount; ++colNumber) {
							SqlColumn column = columnNames.get(colNumber - 1);
							String columnName = column.getName();
							Object value = rs.getObject(columnName);
							values.add(value);
						}

						DbRow row = DbRow.create(values);
						rows.add(row);

					}
					KV<SqlTable, List<DbRow>> kv = KV.of(msSqlTable, rows);
					c.output(kv);

				} catch (SQLException e) {

					if (statement != null) {
						statement.close();
					}

				}

			}

			else {
				// mock object
				List<Object> values = new ArrayList<Object>();
				values.add("myname");
				values.add("10");
				DbRow row = DbRow.create(values);
				rows.add(row);
				c.output(KV.of(msSqlTable, rows));
			}
		}

	}

	@StartBundle
	public void startBundle() throws SQLException {

		if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
			this.connection = ServerUtil.getConnection(this.connectionString.get());
		}

	}

	@FinishBundle
	public void finishBundle() throws Exception {
		if (connection != null) {
			connection.close();
		}
	}

	@GetInitialRestriction
	public OffsetRange getInitialRestriction(SqlTable table)
			throws IOException, GeneralSecurityException, SQLException {

		if (!this.connectionString.get().equals(String.valueOf("TEST_HOST"))) {
			startBundle();
			this.tableCount = ServerUtil.getRowCount(this.connection, null, table.getName());
			this.batchSize = (int) Math.ceil(tableCount / (float) this.offsetCount.get());
			LOG.info("****Table Name {} **** Total Number of Split**** {} **** Total row count {} **** ",
					table.getFullName(), batchSize, tableCount);
			return new OffsetRange(1, batchSize + 1);
		} else {
			return new OffsetRange(1, 2);

		}
	}

	@SplitRestriction
	public void splitRestriction(SqlTable table, OffsetRange range, OutputReceiver<OffsetRange> out) {
		for (final OffsetRange p : range.split(1, 1)) {
			out.output(p);

		}
	}

	@NewTracker
	public OffsetRangeTracker newTracker(OffsetRange range) {
		return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));

	}

}
