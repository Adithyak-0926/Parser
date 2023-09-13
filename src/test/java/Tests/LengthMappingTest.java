/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Tests;

import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
//import org.junit.jupiter.api.Test;
import parsing.SqlQueryPlanner;
import parsing.parsing.SchemaCustom;

public class LengthMappingTest {

    @Test
    public void Lengthmapping() throws Exception {

//        "SELECT e.name AS employee_name, d.name AS department_name\n" +
//                "FROM customSchema.emps e\n" +
//                "JOIN customSchema.depts d ON e.deptno = d.deptno\n"

        String sqlQuery = "SELECT Len('AKA',5), CharLen(5,'AKA')";
//        String sqlQuery = "SELECT CHAR_LENGTH(name) AS LengthOfName\n" +
//                "FROM emps\n";

        CalciteSchema schema = CalciteSchema.createRootSchema(true);
//        SchemaCustom customSchema = new SchemaCustom();
//        schema.add("CustomSchema",customSchema);

        SqlQueryPlanner queryPlanner = new SqlQueryPlanner();
        queryPlanner.getQueryPlan(schema.name,sqlQuery);
    }

}
