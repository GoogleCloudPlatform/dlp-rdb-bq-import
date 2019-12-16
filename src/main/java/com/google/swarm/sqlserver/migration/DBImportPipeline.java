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
package com.google.swarm.sqlserver.migration;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.google.swarm.sqlserver.migration.common.BigQueryTableDestination;
import com.google.swarm.sqlserver.migration.common.BigQueryTableRowDoFn;
import com.google.swarm.sqlserver.migration.common.CreateTableMapDoFn;
import com.google.swarm.sqlserver.migration.common.DBImportPipelineOptions;
import com.google.swarm.sqlserver.migration.common.DLPTokenizationDoFn;
import com.google.swarm.sqlserver.migration.common.DeterministicKeyCoder;
import com.google.swarm.sqlserver.migration.common.SqlTable;
import com.google.swarm.sqlserver.migration.common.TableToDbRowFn;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBImportPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(DBImportPipeline.class);

  public static void main(String[] args) throws IOException, GeneralSecurityException {

    DBImportPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DBImportPipelineOptions.class);

    runDBImport(options);
  }

  @SuppressWarnings("serial")
  public static void runDBImport(DBImportPipelineOptions options) {

    Pipeline p = Pipeline.create(options);

    p.getCoderRegistry()
        .registerCoderProvider(
            CoderProviders.fromStaticMethods(SqlTable.class, DeterministicKeyCoder.class));

    PCollection<ValueProvider<String>> jdbcString =
        p.apply("Check DB Properties", Create.of(options.getJDBCSpec()));

    PCollectionTuple tableCollection =
        jdbcString.apply(
            "Create Table Map",
            ParDo.of(
                    new CreateTableMapDoFn(
                        options.getExcludedTables(),
                        options.getDLPConfigBucket(),
                        options.getDLPConfigObject(),
                        options.getJDBCSpec(),
                        options.getDataSet(),
                        options.as(GcpOptions.class).getProject()))
                .withOutputTags(
                    CreateTableMapDoFn.successTag,
                    TupleTagList.of(CreateTableMapDoFn.deadLetterTag)));

    PCollectionTuple dbRowKeyValue =
        tableCollection
            .get(CreateTableMapDoFn.successTag)
            .apply(
                "Create DB Rows",
                ParDo.of(new TableToDbRowFn(options.getJDBCSpec(), options.getOffsetCount()))
                    .withOutputTags(
                        TableToDbRowFn.successTag, TupleTagList.of(TableToDbRowFn.deadLetterTag)));

    PCollection<KV<SqlTable, TableRow>> successRecords =
        dbRowKeyValue
            .get(TableToDbRowFn.successTag)
            .apply(
                "DLP Tokenization",
                ParDo.of(new DLPTokenizationDoFn(options.as(GcpOptions.class).getProject())))
            .apply("Convert To BQ Row", ParDo.of(new BigQueryTableRowDoFn()))
            .apply(
                Window.<KV<SqlTable, TableRow>>into(FixedWindows.of(Duration.standardSeconds(30)))
                    .triggering(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    WriteResult writeResult =
        successRecords.apply(
            "Write to BQ",
            BigQueryIO.<KV<SqlTable, TableRow>>write()
                .to(new BigQueryTableDestination(options.getDataSet()))
                .withFormatFunction(
                    new SerializableFunction<KV<SqlTable, TableRow>, TableRow>() {

                      @Override
                      public TableRow apply(KV<SqlTable, TableRow> kv) {
                        return kv.getValue();
                      }
                    })
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withoutValidation()
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    writeResult
        .getFailedInserts()
        .apply(
            "LOG BQ Failed Inserts",
            ParDo.of(
                new DoFn<TableRow, TableRow>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    LOG.error("***ERROR*** FAILED INSERT {}", c.element().toString());
                    c.output(c.element());
                  }
                }));

    PCollectionList.of(
            ImmutableList.of(
                tableCollection.get(CreateTableMapDoFn.deadLetterTag),
                dbRowKeyValue.get(TableToDbRowFn.deadLetterTag)))
        .apply("Flatten", Flatten.pCollections())
        .apply(
            "Write Log Errors",
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    LOG.error("***ERROR*** DEAD LETTER TAG {}", c.element().toString());
                    c.output(c.element());
                  }
                }));

    p.run();
  }
}
