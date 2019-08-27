/* Copyright 2019 Google LLC

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
package com.google.swarm.dlp.sql.reid;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.swarm.sqlserver.migration.common.DynamicJdbcIO;
import com.google.swarm.sqlserver.migration.common.JdbcConverters;

public class DLPReidentificationPipeline {
	public static final Logger LOG = LoggerFactory.getLogger(DLPReidentificationPipeline.class);

	public static void main(String[] args) {

		ReIdentificationPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ReIdentificationPipelineOptions.class);
		run(options);
	}

	public static PipelineResult run(ReIdentificationPipelineOptions options) {
		Pipeline p = Pipeline.create(options);
		p.apply("Read from JdbcIO", DynamicJdbcIO.<TableRow>read()
				.withDataSourceConfiguration(
						DynamicJdbcIO.DynamicDataSourceConfiguration.create(options.getSource(), options.getJdbcSpec())
						.withDriverJars(options.getSource()))
				
				.withQuery(options.getQuery()).withCoder(TableRowJsonCoder.of())
				.withRowMapper(JdbcConverters.getResultSetToTableRow()));
		return p.run();

	}

	public interface ReIdentificationPipelineOptions extends DataflowPipelineOptions {

		@Description("DLP Deidentify Template to be used for API request "
				+ "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
		@Required
		String getDeidentifyTemplateName();

		void setDeidentifyTemplateName(String value);

		@Description("JDBC Spec")
		String getJdbcSpec();

		void setJdbcSpec(ValueProvider<String> value);

		@Description("Type of Source")
		String getSource();

		void setSource(String value);


		@Description("Query to run")
		String getQuery();

		void setQuery(String value);
	}

}
