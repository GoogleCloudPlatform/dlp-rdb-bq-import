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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class CreateTableMapDoFn extends DoFn<ValueProvider<String>, SqlTable> {

	private static final Logger LOG = LoggerFactory.getLogger(CreateTableMapDoFn.class);

	private ValueProvider<String> excludedTables;
	private ValueProvider<String> dlpConfigBucket;
	private ValueProvider<String> dlpConfigObject;
	private ValueProvider<String> jdbcSpec;
	private ValueProvider<String> dataset;
	private String projectId;

	private Connection connection = null;

	public CreateTableMapDoFn(ValueProvider<String> excludedTables, ValueProvider<String> dlpConfigBucket,
			ValueProvider<String> dlpConfigObject, ValueProvider<String> jdbcSpec, ValueProvider<String> dataset,
			String projectId) {
		this.excludedTables = excludedTables;
		this.dlpConfigBucket = dlpConfigBucket;
		this.dlpConfigObject = dlpConfigObject;
		this.jdbcSpec = jdbcSpec;
		this.dataset = dataset;
		this.connection = null;
		this.projectId = projectId;

	}

	@Setup
	public void setup() {
		if (this.dataset.isAccessible() && this.dataset.get() != null) {

			BigqueryClient bqClient = new BigqueryClient("DBImportPipeline");
			bqClient.createNewDataset(projectId, this.dataset.get());
			LOG.info("Dataset {} Created Successfully For Project {}", projectId, this.dataset.get());
		}
	}

	@StartBundle
	public void startBundle() throws SQLException {

		if (!this.jdbcSpec.get().equals(String.valueOf("TEST_HOST"))) {
			this.connection = ServerUtil.getConnection(this.jdbcSpec.get());
		}

	}

	@FinishBundle
	public void finishBundle() throws Exception {
		if (connection != null) {
			connection.close();
		}
	}

	@ProcessElement
	public void processElement(ProcessContext c) throws SQLException {

		if (this.jdbcSpec.get() != null) {

			List<SqlTable> tables = new ArrayList<>();
			if (!this.jdbcSpec.get().equals(String.valueOf("TEST_HOST"))) {
				final List<DLPProperties> dlpConfigList = ServerUtil.parseDLPconfig(this.dlpConfigBucket,
						this.dlpConfigObject);

				if (this.excludedTables.isAccessible() && this.excludedTables.get() != null) {
					tables = ServerUtil.getTablesList(this.connection, this.excludedTables.get(), dlpConfigList);

				} else {
					tables = ServerUtil.getTablesList(this.connection, dlpConfigList);

				}

				for (SqlTable table : tables) {
					LOG.debug("Extracting table schema: " + table.getFullName());
					List<SqlColumn> tableColumns = ServerUtil.getColumnsList(connection, table.getName());
					table.setCloumnList(tableColumns);
					c.output(table);
				}

			} else {

				c.output(TestUtil.getMockData());
			}

		}
	}
}
