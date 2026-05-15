/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImpalaSqlIdentifiersTest
{
    @Test
    public void quoteIdentifier_escapesBackticks()
    {
        assertEquals("`a`", ImpalaSqlIdentifiers.quoteIdentifier("a"));
        assertEquals("`a``b`", ImpalaSqlIdentifiers.quoteIdentifier("a`b"));
    }

    @Test
    public void qualifiedTableForMetadataSql_uppercasesAndQuotes()
    {
        TableName tn = new TableName("testSchema", "testTable");
        assertEquals("`TESTSCHEMA`.`TESTTABLE`", ImpalaSqlIdentifiers.qualifiedTableForMetadataSql(tn));
    }

    @Test
    public void qualifiedTableForMetadataSql_adversarialNameContainedInQuotes()
    {
        TableName tn = new TableName("demo_security", "test; INSERT INTO x VALUES (1);--");
        String qualified = ImpalaSqlIdentifiers.qualifiedTableForMetadataSql(tn);
        assertEquals("`DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`", qualified);
        assertEquals("describe FORMATTED `DEMO_SECURITY`.`TEST; INSERT INTO X VALUES (1);--`",
                ImpalaMetadataHandler.GET_METADATA_QUERY + qualified);
    }
}
