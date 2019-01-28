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

import java.sql.SQLException;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryTableDestination extends DynamicDestinations<KV<SqlTable, TableRow>, KV<String, SqlTable>> {

	private static final long serialVersionUID = -2929752032929427146L;

	private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableDestination.class);

	private ValueProvider<String> datasetName;

	public BigQueryTableDestination(ValueProvider<String> datasetName) {
		this.datasetName = datasetName;

	}

	@Override
	public KV<String, SqlTable> getDestination(ValueInSingleWindow<KV<SqlTable, TableRow>> element) {
		KV<SqlTable, TableRow> kv = element.getValue();
		SqlTable table = kv.getKey();
		String key = datasetName.get() + "." + table.getFullName();
		return KV.of(key, table);

	}

	@Override
	public TableDestination getTable(KV<String, SqlTable> value) {
		LOG.info("Table Defination:{}", value.getKey());
		return new TableDestination(value.getKey(), "DB Import Table");
	}

	@Override
	public TableSchema getSchema(KV<String, SqlTable> value) {

		TableSchema tableSchema = null;
		try {

			tableSchema = ServerUtil.getBigQuerySchema(value.getValue().getCloumnList());
			LOG.info("***Table Schema {}", tableSchema.toString());

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tableSchema;

	}

}