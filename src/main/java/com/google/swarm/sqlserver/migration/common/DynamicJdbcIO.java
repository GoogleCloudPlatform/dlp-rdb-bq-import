package com.google.swarm.sqlserver.migration.common;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;




public class DynamicJdbcIO {
	  private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcIO.class);
	  public static final Map<Object, Object> driverNameMap = ImmutableMap.builder()
			  .put(String.valueOf("BQ"), String.valueOf("com.simba.googlebigquery.jdbc42.Driver"))
			  .put(String.valueOf("CLOUD-MYSQL"), String.valueOf("com.simba.googlebigquery.jdbc42.Driver"))
			  .build();
	  public static <T> DynamicRead<T> read() {
		    return new AutoValue_DynamicJdbcIO_DynamicRead.Builder<T>().build();
		  }	
	 
	  @AutoValue
	  public abstract static class DynamicRead<T> extends PTransform<PBegin, PCollection<T>> {
	    @Nullable
	    abstract DynamicDataSourceConfiguration getDynamicDataSourceConfiguration();

	    @Nullable
	    abstract String getQuery();

	    @Nullable
	    abstract JdbcIO.RowMapper<T> getRowMapper();

	    @Nullable
	    abstract Coder<T> getCoder();

	    abstract Builder<T> toBuilder();

	    @AutoValue.Builder
	    abstract static class Builder<T> {
	      abstract Builder<T> setDynamicDataSourceConfiguration(DynamicDataSourceConfiguration config);

	      abstract Builder<T> setQuery(String query);

	      abstract Builder<T> setRowMapper(JdbcIO.RowMapper<T> rowMapper);

	      abstract Builder<T> setCoder(Coder<T> coder);

	      abstract DynamicRead<T> build();
	    }

	    public DynamicRead<T> withDataSourceConfiguration(
	        DynamicDataSourceConfiguration configuration) {
	     
	      return toBuilder().setDynamicDataSourceConfiguration(configuration).build();
	    }

	    public DynamicRead<T> withQuery(String query) {
	      return toBuilder().setQuery(query).build();
	    }

	    public DynamicRead<T> withRowMapper(JdbcIO.RowMapper<T> rowMapper) {
	      return toBuilder().setRowMapper(rowMapper).build();
	    }

	    public DynamicRead<T> withCoder(Coder<T> coder) {
	      return toBuilder().setCoder(coder).build();
	    }

	    @Override
	    public PCollection<T> expand(PBegin input) {
	      return input
	          .apply(Create.of((Void) null))
	          .apply(
	              ParDo.of(
	                  new DynamicReadFn<>(
	                      getDynamicDataSourceConfiguration(), getQuery(), getRowMapper())))
	          .setCoder(getCoder())
	          .apply(new Reparallelize<>());
	    }
	  } 
	  
	  @AutoValue
	  public abstract static class DynamicDataSourceConfiguration implements Serializable {
	    @Nullable
	    abstract String getDriverClassName();

	    @Nullable
	    abstract String getUrl();

	    @Nullable
	    abstract String getDriverJars();

	    abstract Builder builder();

	    @AutoValue.Builder
	    abstract static class Builder {
	      abstract Builder setDriverClassName(String driverClassName);
	      abstract Builder setUrl(String url);
	      abstract Builder setDriverJars(String jars);
	      abstract DynamicDataSourceConfiguration build();
	    }

	    public static DynamicDataSourceConfiguration create(String url, String sourceType ) {
	     
	      return new AutoValue_DynamicJdbcIO_DynamicDataSourceConfiguration.Builder()
	          .setDriverClassName(driverNameMap.get(sourceType).toString())
	          .setUrl(url)
	          .build();
	    }

	   

	    public DynamicDataSourceConfiguration withDriverJars(String driverJars) {
	     
	      return builder().setDriverJars(driverJars).build();
	    }

	    
	    DataSource buildDatasource() {
		      BasicDataSource basicDataSource = new BasicDataSource();
		      if (getDriverClassName() != null) {
		        if (getDriverClassName() == null) {
		          throw new RuntimeException("Driver class name is required.");
		        }
		        basicDataSource.setDriverClassName(driverNameMap.get(getDriverClassName().toString()).toString());
		      }
		      if (getUrl() != null) {
		        if (getUrl() == null) {
		          throw new RuntimeException("Connection url is required.");
		        }
		        basicDataSource.setUrl(getUrl());
		      }
		      
		      if (getDriverJars() != null) {
		        ClassLoader urlClassLoader = getNewClassLoader(getDriverJars());
		        basicDataSource.setDriverClassLoader(urlClassLoader);
		      }

		      DataSourceConnectionFactory connectionFactory =
		          new DataSourceConnectionFactory(basicDataSource);
		      PoolableConnectionFactory poolableConnectionFactory =
		          new PoolableConnectionFactory(connectionFactory, null);
		      GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		      poolConfig.setMaxTotal(1);
		      poolConfig.setMinIdle(0);
		      poolConfig.setMinEvictableIdleTimeMillis(10000);
		      poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
		      GenericObjectPool connectionPool =
		          new GenericObjectPool(poolableConnectionFactory, poolConfig);
		      poolableConnectionFactory.setPool(connectionPool);
		      poolableConnectionFactory.setDefaultAutoCommit(false);
		      poolableConnectionFactory.setDefaultReadOnly(false);
		      PoolingDataSource poolingDataSource = new PoolingDataSource(connectionPool);
		      return poolingDataSource;
		    }

		private static URLClassLoader getNewClassLoader(String paths) {
			
			paths = String.format("%s/GoogleBigQueryJDBC42.jar", paths);
			LOG.info("Paths {}", paths);
			List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(paths);

		      final String destRoot = Files.createTempDir().getAbsolutePath();
		      URL[] urls =
		          listOfJarPaths.stream().map(jarPath->{

		        	  ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                      File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                      ResourceId destResourceId =
                          FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                      LOG.info("Source {}, File {} , Dest {}",sourceResourceId.toString(),destFile.toString(),destResourceId.getFilename().toString());
                      try {
						copy(sourceResourceId, destResourceId);
					} catch (IOException e) {
						
						 throw new RuntimeException(e);
					}
	
                      try {
						return destFile.toURI().toURL();
					} catch (MalformedURLException e) {
					
							 throw new RuntimeException(e);
					}
		          }).toArray(URL[]::new);
		             

		      return URLClassLoader.newInstance(urls);
		}
		 /** utility method to copy binary (jar file) data from source to dest. */
	    private static void copy(ResourceId source, ResourceId dest) throws IOException {
	      try (ReadableByteChannel rbc = FileSystems.open(source)) {
	        try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
	          ByteStreams.copy(rbc, wbc);
	        }
	      }
	    }
	    	
	   
  }

		 
	 
	  

	  private static class DynamicReadFn<X, T> extends DoFn<X, T> {

		    private final DynamicDataSourceConfiguration dataSourceConfiguration;
		    private final String query;
		    private final JdbcIO.RowMapper<T> rowMapper;

		    private DataSource dataSource;
		    private Connection connection;

		    private DynamicReadFn(
		        DynamicDataSourceConfiguration dataSourceConfiguration,
		        String query,
		        JdbcIO.RowMapper<T> rowMapper) {
		      this.dataSourceConfiguration = dataSourceConfiguration;
		      this.query = query;
		      this.rowMapper = rowMapper;
		    }

		    @Setup
		    public void setup() throws Exception {
		      dataSource = dataSourceConfiguration.buildDatasource();
		      connection = dataSource.getConnection();
		    }

		    @ProcessElement
		    public void processElement(ProcessContext context) throws Exception {
		      try (PreparedStatement statement = connection.prepareStatement(query)) {
		        try (ResultSet resultSet = statement.executeQuery()) {
		          while (resultSet.next()) {
		            context.output(rowMapper.mapRow(resultSet));
		          }
		        }
		      }
		    }

		    @Teardown
		    public void teardown() throws Exception {
		      connection.close();
		      if (dataSource instanceof AutoCloseable) {
		        ((AutoCloseable) dataSource).close();
		      }
		    }
		  }


	  private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
		    @Override
		    public PCollection<T> expand(PCollection<T> input) {
		      
		      PCollectionView<Iterable<T>> empty =
		          input
		              .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
		              .apply(View.asIterable());
		      PCollection<T> materialized =
		          input.apply(
		              "Identity",
		              ParDo.of(
		                      new DoFn<T, T>() {
		                        @ProcessElement
		                        public void process(ProcessContext c) {
		                          c.output(c.element());
		                        }
		                      })
		                  .withSideInputs(empty));
		      return materialized.apply(Reshuffle.viaRandomKey());
		    }

	  }
	
		 
}
