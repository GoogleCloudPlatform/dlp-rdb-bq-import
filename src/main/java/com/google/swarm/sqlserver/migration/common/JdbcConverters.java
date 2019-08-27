package com.google.swarm.sqlserver.migration.common;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public class JdbcConverters {
 
	  /** Factory method for {@link ResultSetToTableRow}. */
	  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
	    return new ResultSetToTableRow();
	  }

	  /**
	   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
	   */
	  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

	    @Override
	    public TableRow mapRow(ResultSet resultSet) throws Exception {

	      ResultSetMetaData metaData = resultSet.getMetaData();

	      TableRow outputTableRow = new TableRow();

	      for (int i = 1; i <= metaData.getColumnCount(); i++) {
	        outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
	      }

	      return outputTableRow;
	    }
	  }
	}
