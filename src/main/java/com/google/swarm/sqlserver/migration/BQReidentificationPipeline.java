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
package com.google.swarm.sqlserver.migration;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.ReidentifyContentRequest;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Table.Builder;
import com.google.privacy.dlp.v2.Table.Row;
import com.google.privacy.dlp.v2.Value;
import com.google.swarm.sqlserver.migration.common.BQReIdentificationPipelineOptions;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BQReidentificationPipeline} is a batch pipeline that reads data from BigQuery Table,
 * call DLP API to Re-Identify data based on the DLP templates and publish the data in a PubSub
 * Topic.
 */
public class BQReidentificationPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(DBImportPipeline.class);
  /** PubSub configuration for default batch size in number of messages */
  public static final Integer PUB_SUB_BATCH_SIZE = 1000;
  /** PubSub configuration for default batch size in bytes */
  public static final Integer PUB_SUB_BATCH_SIZE_BYTES = 10000;

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * BQReidentificationPipeline#run(BQReIdentificationPipelineOptions)} method to start the pipeline
   * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String args[]) {
    BQReIdentificationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BQReIdentificationPipelineOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static void run(BQReIdentificationPipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, Iterable<Row>>> dlpRows =
        p.apply(
                "Read from BQ",
                BigQueryIO.readTableRows()
                    .fromQuery(options.getQuery())
                    .usingStandardSql()
                    .withMethod(Method.DEFAULT))
            .apply("MapToDLPTableRow", MapElements.via(new ConvertToDLPTableRow()))
            .apply("Group By Header", GroupByKey.create());

    final PCollectionView<List<String>> headerMap =
        dlpRows
            .apply("Create Header Map", ParDo.of(new CreateHeaderMap(options.getColumnMap())))
            .apply("View As List", View.asList());

    dlpRows
        .apply(
            "DLP-ReIdentification",
            ParDo.of(
                    new DLPReIdentificationDoFn(
                        options.getDeidentifyTemplateName(),
                        options.getInspectTemplateName(),
                        options.getProject(),
                        headerMap))
                .withSideInputs(headerMap))
        .apply(
            "Publish Events to PubSub",
            PubsubIO.writeMessages()
                .withMaxBatchBytesSize(PUB_SUB_BATCH_SIZE_BYTES)
                .withMaxBatchSize(PUB_SUB_BATCH_SIZE)
                .to(options.getTopic()));

    p.run();
  }

  /* Convert BigQyery Table Row to DLP Table Row */
  public static class ConvertToDLPTableRow extends SimpleFunction<TableRow, KV<String, Table.Row>> {
    @Override
    public KV<String, Table.Row> apply(TableRow row) {
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
      List<String> headerKeys = new ArrayList<>();

      row.entrySet()
          .forEach(
              element -> {
                String key = element.getKey();
                String value = element.getValue().toString();
                tableRowBuilder.addValues(
                    Value.newBuilder().setStringValue(value.toString()).build());
                headerKeys.add(key);
              });
      String headerKey = String.join(",", headerKeys);
      LOG.debug("header Key {}", headerKey);
      Table.Row dlpRow = tableRowBuilder.build();
      return KV.of(headerKey, dlpRow);
    }
  }

  @SuppressWarnings("serial")
  /* DLP ReIdentification Process Transform */

  static class DLPReIdentificationDoFn extends DoFn<KV<String, Iterable<Row>>, PubsubMessage> {

    private DlpServiceClient dlpServiceClient;
    private String deidTemplate;
    private String inspectTemplate;
    private Builder dlpTableBuilder;
    private String projectId;
    private PCollectionView<List<String>> headerMap;
    private Gson gson;
    private ReidentifyContentRequest.Builder requestBuilder;
    private boolean inspectTemplateExist;

    public DLPReIdentificationDoFn(
        String deidTemplate,
        String inspectTemplate,
        String projectId,
        PCollectionView<List<String>> headerMap) {
      this.deidTemplate = deidTemplate;
      this.projectId = projectId;
      this.inspectTemplate = inspectTemplate;
      this.headerMap = headerMap;
      this.inspectTemplateExist = false;
    }

    @Setup
    public void setup() {
      dlpTableBuilder = Table.newBuilder();
      gson = new Gson();
      if (this.inspectTemplate != null) {

        this.inspectTemplateExist = true;
      }
      if (this.deidTemplate != null) {

        this.requestBuilder =
            ReidentifyContentRequest.newBuilder()
                .setParent(ProjectName.of(projectId).toString())
                .setReidentifyTemplateName(deidTemplate);

        if (this.inspectTemplateExist) {
          this.requestBuilder.setInspectTemplateName(this.inspectTemplate);
        }
      }
    }

    @StartBundle
    public void startBundle() throws SQLException {

      try {
        this.dlpServiceClient = DlpServiceClient.create();

      } catch (IOException e) {
        LOG.error("Failed to create DLP Service Client", e.getMessage());
        throw new RuntimeException(e);
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (this.dlpServiceClient != null) {
        this.dlpServiceClient.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Iterable<Row>> rows = c.element();
      List<FieldId> dlpTableHeaders =
          c.sideInput(headerMap).stream()
              .map(header -> FieldId.newBuilder().setName(header).build())
              .collect(Collectors.toList());

      Table dlpTable =
          dlpTableBuilder.addAllHeaders(dlpTableHeaders).addAllRows(rows.getValue()).build();
      ContentItem tableItem = ContentItem.newBuilder().setTable(dlpTable).build();
      requestBuilder.setItem(tableItem);
      LOG.info("Item Size {}", tableItem.getSerializedSize());
      ReidentifyContentResponse response =
          dlpServiceClient.reidentifyContent(requestBuilder.build());

      List<Table.Row> outputRows = response.getItem().getTable().getRowsList();

      outputRows.forEach(
          row -> {
            HashMap<String, String> convertMap = new HashMap<String, String>();
            AtomicInteger index = new AtomicInteger(0);
            row.getValuesList()
                .forEach(
                    value -> {
                      String header = dlpTableHeaders.get(index.getAndIncrement()).getName();

                      LOG.debug("Header {}, Values {}", header, value.getStringValue());

                      convertMap.put(header, value.getStringValue());
                    });
            String jsonMessage = gson.toJson(convertMap);
            LOG.debug("Json message {}", jsonMessage);
            PubsubMessage message = new PubsubMessage(jsonMessage.toString().getBytes(), null);
            c.output(message);
          });
    }
  }

  @SuppressWarnings("serial")
  /* Side Input as List */

  static class CreateHeaderMap extends DoFn<KV<String, Iterable<Row>>, String> {

    private String jsonColumnMap;
    private JsonObject columnMap;

    public CreateHeaderMap(String jsonColumnMap) {
      this.jsonColumnMap = jsonColumnMap;
    }

    @Setup
    public void setup() {
      if (jsonColumnMap != null) {
        columnMap = new Gson().fromJson(jsonColumnMap, JsonObject.class);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<String> bqHeaders = Arrays.asList(c.element().getKey().split(","));
      List<String> orginalHeaders = new ArrayList<>();
      if (columnMap != null) {
        Set<Entry<String, JsonElement>> mapColumns = columnMap.entrySet();
        bqHeaders.forEach(
            bqHeader -> {
              JsonElement originalHeader =
                  mapColumns.stream()
                      .filter(k -> k.getKey().equals(bqHeader))
                      .map(Map.Entry::getValue)
                      .findFirst()
                      .orElse(null);
              if (originalHeader != null) {
                orginalHeaders.add(originalHeader.getAsString());
              } else {
                orginalHeaders.add(bqHeader);
              }
            });

      } else {
        orginalHeaders.addAll(bqHeaders);
      }
      orginalHeaders.forEach(
          header -> {
            c.output(header);
          });
    }
  }
}
