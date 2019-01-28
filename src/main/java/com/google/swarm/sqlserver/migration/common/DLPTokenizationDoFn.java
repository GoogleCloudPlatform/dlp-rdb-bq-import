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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

@SuppressWarnings("serial")
public class DLPTokenizationDoFn extends DoFn<KV<SqlTable, List<DbRow>>, KV<SqlTable, DbRow>> {

	private static final Logger LOG = LoggerFactory.getLogger(DLPTokenizationDoFn.class);
	private String projectId;
	private DlpServiceClient dlpServiceClient;
	private static final String EMPTY_STRING = "";

	public DLPTokenizationDoFn(String projectId) {
		this.projectId = projectId;
		dlpServiceClient = null;

	}

	@StartBundle
	public void startBundle() throws SQLException {

		try {
			this.dlpServiceClient = DlpServiceClient.create();
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	@FinishBundle
	public void finishBundle() throws Exception {
		if (this.dlpServiceClient != null) {
			this.dlpServiceClient.close();
		}
	}

	@ProcessElement
	public void processElement(ProcessContext c) throws GeneralSecurityException {
		KV<SqlTable, List<DbRow>> nonTokenizedData = c.element();
		SqlTable tableId = nonTokenizedData.getKey();
		List<DbRow> rows = nonTokenizedData.getValue();
		DLPProperties prop = tableId.getDlpConfig();

		if (prop != null) {
			List<Table.Row> dlpRows = new ArrayList<>();
			rows.forEach(row -> {
				List<Object> fields = row.fields();
				Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
				for (int i = 0; i < fields.size(); i++) {
					Object fieldData = fields.get(i);
					if (fieldData != null) {
						tableRowBuilder.addValues(Value.newBuilder().setStringValue(fieldData.toString()).build());
					} else {
						// null value fix
						tableRowBuilder.addValues(Value.newBuilder().setStringValue(EMPTY_STRING).build());

					}
				}
				dlpRows.add(tableRowBuilder.build());

			});

			Table table = Table.newBuilder().addAllHeaders(ServerUtil.getHeaders(tableId.getCloumnList()))
					.addAllRows(dlpRows).build();
			dlpRows.clear();
			ContentItem tableItem = ContentItem.newBuilder().setTable(table).build();
			DeidentifyContentRequest request;
			DeidentifyContentResponse response;
			if (prop.getInspTemplate() != null) {
				request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
						.setDeidentifyTemplateName(prop.getDeidTemplate())
						.setInspectTemplateName(prop.getInspTemplate()).setItem(tableItem).build();
			} else {
				request = DeidentifyContentRequest.newBuilder().setParent(ProjectName.of(this.projectId).toString())
						.setDeidentifyTemplateName(prop.getDeidTemplate()).setItem(tableItem).build();

			}

			response = dlpServiceClient.deidentifyContent(request);

			Table encryptedData = response.getItem().getTable();

			LOG.info("Request Size Successfully Tokenized: " + request.toByteString().size() + " bytes."
					+ " Number of rows tokenized: " + response.getItem().getTable().getRowsCount());

			List<Table.Row> outputRows = encryptedData.getRowsList();
			List<Value> values = new ArrayList<Value>();
			List<String> encrytedValues = new ArrayList<String>();

			for (Table.Row outputRow : outputRows) {

				values = outputRow.getValuesList();

				values.forEach(value -> {

					encrytedValues.add(value.getStringValue());

				});

				List<Object> objectList = new ArrayList<Object>(encrytedValues);
				DbRow row = DbRow.create(objectList);
				c.output(KV.of(tableId, row));
				encrytedValues.clear();

			}

		} else {
			rows.forEach(row -> {

				c.output(KV.of(tableId, row));
			});
		}
	}
}
