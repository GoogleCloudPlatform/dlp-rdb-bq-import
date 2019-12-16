package com.google.swarm.sqlserver.migration.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface BQReIdentificationPipelineOptions extends DataflowPipelineOptions {

  @Description("BQ Dataset")
  String getDataSet();

  void setDataSet(String value);

  @Description("Query to execute")
  @Default.String(
      "select id,card_number,Card_Holders_Name from `s3-dlp-experiment.pii_dataset.CCRecords_*` where cast(credit_limit as int64)>100000 and cast (age as int64) > 50 group by id,card_number,Card_Holders_Name")
  String getQuery();

  void setQuery(String value);

  @Description("DLP DeIdentify Template")
  String getDeidentifyTemplateName();

  void setDeidentifyTemplateName(String value);

  @Description("DLP Inspect Template")
  String getInspectTemplateName();

  void setInspectTemplateName(String value);

  @Description("PUB Sub Topic")
  String getTopic();

  void setTopic(String value);

  @Description("Mapping of a BQ columns and Original Column names")
  @Default.String(
      "{\"card_number\": \"Card Number\", \"Card_Holders_Name\": \"Card Holder's Name\"}")
  String getColumnMap();

  void setColumnMap(String value);
}
