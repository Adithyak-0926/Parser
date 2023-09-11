package parsing;
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


import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;


import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * Query processor for running TPC-H queries over Apache Lucene.
 */
public class LuceneQueryProcessor {

  /**
   * The type of query processor.
   */
  public enum Type {
    /**
     * Simple query processor using only one convention.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and the {@link org.apache.calcite.schema.ScannableTable} interface to execute queries over
     * Lucene.
     */
    SIMPLE,
    /**
     * Advanced query processor using two conventions.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and  to execute queries over Lucene.
     */
    ADVANCED,
    /**
     * Advanced query processor using two conventions and extra rules capable of pushing basic
     * conditions in Lucene.
     *
     * The processor relies on {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}
     * and to execute queries over Lucene.
     */
    PUSHDOWN
  }

  public static void main(String[] args) throws Exception {
//    if (args.length != 2) {
//      System.out.println("Usage: runner [SIMPLE|ADVANCED] SQL_FILE");
//      System.exit(-1);
//    }
//    Type pType = Type.valueOf(args[0]);
//    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[1])), StandardCharsets.UTF_8);
//Reads the INPUT QUERY WE ARE GIVING AND STORES IT INTO sqlQuery

//    SELECT CONVERT_TIMEZONE('America/Los_Angeles', TIMESTAMP '2008-12-25 05:30:00'),ABS(1.8),1
//    SELECT ADD(2,4),SUB(2,5)


    String sqlQuery = "SELECT Len('AKA',5), CharLen(5,'AKA')";




    System.out.println("[Results]");
    long start = System.currentTimeMillis();  // Taking the starting time snapshot in milliseconds
    for (Object row : execute(sqlQuery, Type.SIMPLE)) {
      if (row instanceof Object[]) {
        System.out.println(Arrays.toString((Object[]) row));   // prints the array of objs into strings using 'row'.
      } else {
        System.out.println(row);
      }
    }
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }
// The above reads the file , execute it, prints it along with time taken for execution in ms
  /**
   * Plans and executes an SQL query.
   *
   * @param sqlQuery - a string with the SQL query for execution
   * @return an Enumerable with the results of the execution of the query
   * @throws SqlParseException if there is a problem when parsing the query
   */
  public static <T> Enumerable<T> execute(String sqlQuery, Type processorType)  //Takes the query as input and also prints it and executes it
      throws SqlParseException {
    System.out.println("[Input query]");
    System.out.println(sqlQuery);    //prints out AST

    // Create the schema and table data types
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();                                        //First, we create an instance of RelDataTypeFactory, which provides SQL type definitions.
    for (TpchTable table : TpchTable.values()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);               //Typefactory - like it is an object of RelDataTypeFactory that contains sql type definations
      for (TpchTable.Column column : table.columns) {
        RelDataType type = typeFactory.createJavaType(column.type);
        builder.add(column.name, type.getSqlTypeName()).nullable(true);
      }
      String indexPath = DatasetIndexer.INDEX_LOCATION + "/tpch/" + table.name();
      schema.add(table.name(), new LuceneTable(indexPath, builder.build()));   // path
    }


    SqlQueryPlanner queryPlanner = new SqlQueryPlanner();
     queryPlanner.getQueryPlan(schema.getName(),sqlQuery);
//    // Create an SQL parser
//    SqlParser parser = SqlParser.create(sqlQuery);
//    // Parse the query into an AST
//    SqlNode sqlNode = parser.parseQuery();  //Parses the query and throws exception if any
//    System.out.println("[Parsed query]");
//    System.out.println(sqlNode.toString());  // Prints out AST
//
//
//
//    // Configure and instantiate validator
//    Properties props = new Properties();
//    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
//    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
//    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,                                     // catalogreader - object that provides access to database objects
//            Collections.singletonList("bs"),
//            typeFactory, config);
//                                                                                                       // we can extend SqlValidator to add any customisations like errors in validations
//    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTableNew.instance(),         //SqlOperatorTable, the library of SQL functions and operators.
//            catalogReader, typeFactory,
//            SqlValidator.Config.DEFAULT);
//
//    // Validate the initial AST
//    SqlNode validNode = validator.validate(sqlNode);
//
//    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
//    RelOptCluster cluster = newCluster(typeFactory);
//
////    RexBuilder rexBuilder = cluster.getRexBuilder();
////    RexShuttleNew customShuttle = new RexShuttleNew(rexBuilder);
//
////    CustomSqlVisitor customVisitor = new CustomSqlVisitor();
////    SqlNode modifiedNode = validNode.accept(customVisitor);
//
//
//    SqlToRelConverter relConverter = new SqlToRelConverter(
//            NOOP_EXPANDER,
//            validator,
//            catalogReader,
//            cluster,
//            StandardConvertletTable.INSTANCE,
//            SqlToRelConverter.config());

    // Convert the valid AST into a logical plan
//    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    // Display the logical plan
//    System.out.println(
//            RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
//                    SqlExplainLevel.NON_COST_ATTRIBUTES));


//    AbstractNode phyPlan = RelToPhysicalPlan.getAbstractNode(logPlan , null) ;

//    System.out.println(phyPlan);

//    ConditionNode condTree = RelFilterTree.getConditionString(logPlan, null);
//    System.out.println("condTree");


//    // Initialize optimizer/planner with the necessary rules
//    RelOptPlanner planner = cluster.getPlanner();                                      //Our inputs are a relational tree to optimize, a set of optimization rules,
//    planner.addRule(CoreRules.PROJECT_TO_CALC);                                                              and traits that the optimized tree's parent node must satisfy.
//    planner.addRule(CoreRules.FILTER_TO_CALC);
//    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
//    planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
//    switch (processorType) {
//    case PUSHDOWN:
//      planner.addRule(LuceneFilterRule.DEFAULT.toRule());
//      // Fall-through
//    case ADVANCED:
//      planner.addRule(LuceneTableScanRule.DEFAULT.toRule());
//      planner.addRule(LuceneToEnumerableConverterRule.DEFAULT.toRule());
//      break;
//    case SIMPLE:
//      planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
//      break;
//    default:
//      throw new AssertionError();
//    }
//
//    // Define the type of the output plan (in this case we want a physical plan in
//    // EnumerableContention)
//    logPlan = planner.changeTraits(logPlan,
//        cluster.traitSet().replace(EnumerableConvention.INSTANCE));
//    planner.setRoot(logPlan);
//    // Start the optimization process to obtain the most efficient physical plan based on the
//    // provided rule set.
//    EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();
//
//    // Display the physical plan
//    System.out.println(
//        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
//            SqlExplainLevel.NON_COST_ATTRIBUTES));
//
//    // Obtain the executable plan
//    Bindable<T> executablePlan = EnumerableInterpretable.toBindable(
//        new HashMap<>(),
//        null,
//        phyPlan,
//        EnumerableRel.Prefer.ARRAY);
//    // Run the executable plan using a context simply providing access to the schema
//    return executablePlan.bind(new SchemaOnlyDataContext(schema));
//  }

    return null;
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  public static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
