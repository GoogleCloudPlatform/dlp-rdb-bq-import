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

import java.util.HashMap;

public enum SqlDataType {
  VARCHAR(0),
  NVARCHAR(1),
  CHAR(2),
  NCHAR(3),
  TEXT(4),
  NTEXT(5),
  BIGINT(6),
  INT(7),
  TINYINT(8),
  SMALLINT(9),
  NUMERIC(10),
  DECIMAL(11),
  MONEY(12),
  SMALLMONEY(13),
  FLOAT(14),
  REAL(15),
  BIT(16),
  DATE(17),
  TIME(18),
  DATETIME(19),
  DATETIME2(20),
  DATETIMEOFFSET(21),
  SMALLDATETIME(22),
  BINARY(23),
  IMAGE(24),
  VARBINARY(25),
  UNIQUEIDENTIFIER(26),
  TIMESTAMP(27);

  private int codeValue;

  private static HashMap<Integer, SqlDataType> codeValueMap = new HashMap<Integer, SqlDataType>();

  private SqlDataType(int codeValue) {
    this.codeValue = codeValue;
  }

  static {
    for (SqlDataType type : SqlDataType.values()) {
      codeValueMap.put(type.codeValue, type);
    }
  }

  public static SqlDataType getInstanceFromCodeValue(int codeValue) {
    return codeValueMap.get(codeValue);
  }

  public int getCodeValue() {
    return codeValue;
  }
}
