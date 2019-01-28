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
package com.google.swarm.sqlserver.migration.test.comoon;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Bucket.BucketSourceOption;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.swarm.sqlserver.migration.DBImportPipeline;
import com.google.swarm.sqlserver.migration.common.DBImportPipelineOptions;
import com.google.swarm.sqlserver.migration.common.TestUtil;

@RunWith(JUnit4.class)
public class DBImportPipelineIT {

	public static final Logger LOG = LoggerFactory.getLogger(DBImportPipelineIT.class);
	public static DBImportPipelineOptions options;
	public static BigqueryClient bqClient;
	public static String projectId;
	public static Storage storage;
	public static Bucket bucket;

	@Before
	public void setupTestEnvironment() {
		PipelineOptionsFactory.register(DBImportPipelineOptions.class);
		options = TestPipeline.testingPipelineOptions().as(DBImportPipelineOptions.class);
		projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
		bqClient = new BigqueryClient("DBImportPipelineIT");
		bqClient.createNewDataset(projectId, StaticValueProvider.of(TestUtil.DATASET).get());
		storage = StorageOptions.getDefaultInstance().getService();
		bucket = storage.create(BucketInfo.of(TestUtil.TEMP_BUCKET));
		TestUtil.TEMP_LOCATION = String.format("%s%s", "gs://", TestUtil.TEMP_BUCKET);
	}

	@After
	public void cleanupTestEnvironment() {
		bqClient.deleteDataset(projectId, StaticValueProvider.of(TestUtil.DATASET).get());
		bucket.delete(BucketSourceOption.metagenerationMatch());

	}

	@Test
	public void testE2EDBImportPipeline() throws Exception {

		options.setDataSet(StaticValueProvider.of(TestUtil.DATASET));
		options.setJDBCSpec(StaticValueProvider.of(TestUtil.JDBC_SPEC));
		options.setTempLocation(TestUtil.TEMP_LOCATION);
		options.setOffsetCount(StaticValueProvider.of(TestUtil.OFFSET));
		DBImportPipeline.runDBImport(options);

		QueryResponse response = bqClient.queryWithRetries(
				String.format("SELECT count(*) as total FROM [%s:%s.%s_%s]", projectId,
						StaticValueProvider.of(TestUtil.DATASET).get(), TestUtil.TABLE_SCHEMA, TestUtil.TABLE_NAME),
				projectId);
		String res = response.getRows().get(0).getF().get(0).getV().toString();
		assertEquals(TestUtil.EXPECTED_RESULT, res);
	}

}
