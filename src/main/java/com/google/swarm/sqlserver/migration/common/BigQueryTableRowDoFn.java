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

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;

import com.google.api.services.bigquery.model.TableRow;

@SuppressWarnings("serial")
public class BigQueryTableRowDoFn extends DoFn<KV<SqlTable, DbRow>, KV<SqlTable, TableRow>> {

	@ProcessElement
	public void processElement(ProcessContext c) {
		SqlTable msSqlTable = c.element().getKey();
		DbRow dbRow = c.element().getValue();
		List<SqlColumn> columnNames = msSqlTable.getCloumnList();
		List<Object> fields = dbRow.fields();
		TableRow bqRow = new TableRow();
		for (int i = 0; i < fields.size(); i++) {
			Object fieldData = fields.get(i);
			if (fieldData == null)
				continue;

			SqlColumn column = columnNames.get(i);
			String columnName = column.getName();

			String sFieldData = fieldData.toString();

			if (column.getDataType().equalsIgnoreCase(SqlDataType.IMAGE.toString())) {
				byte[] bytesEncoded = Base64.encodeBase64(sFieldData.getBytes());
				bqRow.put(columnName, bytesEncoded);
				continue;
			}

			if (!sFieldData.toLowerCase().equals("null"))
				bqRow.put(columnName, sFieldData);
		}
		c.output(KV.of(msSqlTable, bqRow));

	}
}
