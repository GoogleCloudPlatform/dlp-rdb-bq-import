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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@SuppressWarnings("serial")
@DefaultCoder(SerializableCoder.class)
public class SqlTable implements Serializable {
	private String schema;
	private String name;
	private String type;
	private long key;
	@Nullable
	private DLPProperties dlpConfig;
	private List<SqlColumn> cloumnList;

	public SqlTable(long key) {
		this.key = key;
		dlpConfig = null;
		cloumnList = new ArrayList<>();
	}

	public long getKey() {
		return key;
	}

	public void setKey(long key) {
		this.key = key;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFullName() {
		return (getSchema() != null && getSchema().length() > 0 ? getSchema() + "_" + getName() : getName());
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public DLPProperties getDlpConfig() {
		return dlpConfig;
	}

	public void setDlpConfig(DLPProperties dlpConfig) {
		this.dlpConfig = dlpConfig;
	}

	public List<SqlColumn> getCloumnList() {
		return cloumnList;
	}

	public void setCloumnList(List<SqlColumn> cloumnList) {
		this.cloumnList = cloumnList;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cloumnList == null) ? 0 : cloumnList.hashCode());
		result = prime * result + ((dlpConfig == null) ? 0 : dlpConfig.hashCode());
		result = prime * result + (int) (key ^ (key >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SqlTable other = (SqlTable) obj;
		if (cloumnList == null) {
			if (other.cloumnList != null)
				return false;
		} else if (!cloumnList.equals(other.cloumnList))
			return false;
		if (dlpConfig == null) {
			if (other.dlpConfig != null)
				return false;
		} else if (!dlpConfig.equals(other.dlpConfig))
			return false;
		if (key != other.key)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MsSqlTable [schema=" + schema + ", name=" + name + ", type=" + type + ", key=" + key + ", dlpConfig="
				+ dlpConfig + ", cloumnList=" + cloumnList + "]";
	}

}
