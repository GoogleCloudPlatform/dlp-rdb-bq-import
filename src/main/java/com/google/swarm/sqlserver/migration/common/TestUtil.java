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

import java.util.ArrayList;
import java.util.List;

public class TestUtil {

	public final static String DATASET = String.valueOf("TEST_DATA_SET");
	public final static String JDBC_SPEC = String.valueOf("TEST_HOST");
	public final static String EXPECTED_RESULT = String.valueOf("1");
	public final static String TABLE_NAME = String.valueOf("TEST_TABLE");
	public final static String TABLE_SCHEMA = String.valueOf("DBO");
	public final static String TABLE_TYPE = String.valueOf("BASIC");
	public final static int OFFSET = 1;
	public final static String COLUMN1_NAME = String.valueOf("NAME");
	public final static String COLUMN1_TYPE = String.valueOf("VARCHAR");
	public final static String COLUMN2_NAME = String.valueOf("AGE");
	public final static String COLUMN2_TYPE = String.valueOf("INT");
	public static final String TEMP_BUCKET = String.valueOf("test_db_import");
	public static String TEMP_LOCATION = null;

	public final static List<SqlColumn> testColumns = new ArrayList<>();

	public static SqlTable getMockData() {

		SqlColumn column1 = new SqlColumn();
		column1.setDataType(COLUMN1_TYPE);
		column1.setDefaultValue(null);
		column1.setName(COLUMN1_NAME);
		column1.setNullable(true);
		column1.setOrdinalPosition(1);

		SqlColumn column2 = new SqlColumn();
		column2.setDataType(COLUMN2_TYPE);
		column2.setDefaultValue(null);
		column2.setName("COLUMN2_NAME");
		column2.setNullable(true);
		column2.setOrdinalPosition(2);

		testColumns.add(column1);
		testColumns.add(column2);

		SqlTable testTable = new SqlTable(1);
		testTable.setName(TABLE_NAME);
		testTable.setSchema(TABLE_SCHEMA);
		testTable.setType(TABLE_TYPE);
		testTable.setCloumnList(testColumns);
		return testTable;

	}
}
