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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

@SuppressWarnings("serial")
public class DeterministicKeyCoder extends AtomicCoder<SqlTable> {

	public static DeterministicKeyCoder of() {
		return INSTANCE;
	}

	private static final DeterministicKeyCoder INSTANCE = new DeterministicKeyCoder();

	private DeterministicKeyCoder() {
	}

	@Override
	public void encode(SqlTable value, OutputStream outStream) throws CoderException, IOException {

		ObjectOutputStream data = new ObjectOutputStream(outStream);

		data.writeObject(value);

	}

	@Override
	public SqlTable decode(InputStream inStream) throws CoderException, IOException {

		ObjectInputStream data = new ObjectInputStream(inStream);
		SqlTable value = null;
		try {
			value = (SqlTable) data.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return value;

	}

	@Override
	public void verifyDeterministic() {
	}

}
