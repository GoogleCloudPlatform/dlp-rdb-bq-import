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
import java.util.List;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.google.auto.value.AutoValue;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class DbRow implements Serializable {

	private static final long serialVersionUID = -4891579648750861340L;

	public abstract List<Object> fields();

	public static DbRow create(List<Object> fields) {
		return new AutoValue_DbRow(fields);
	}

	@Override
	public String toString() {
		return "DbRow [fields()=" + fields() + "]";
	}

}