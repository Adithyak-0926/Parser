package parsing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;

public class SqlQueryPlanner {

//    public CalciteSchema schema;


    public void getQueryPlan(String sSchemaName, String sQueryString)
    {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);

        //E6Schema schema = m_metadataService.getRootSchema();
        SqlParser1 parser = getPlanner(schema,  sSchemaName, Quoting.DOUBLE_QUOTE);
        parser.parse("", sSchemaName, sQueryString);

    }

    private SqlParser1 getPlanner(CalciteSchema rootSchema, String defaultSchema,
                                   Quoting quoting)
    {
        SqlParser1 parser;
        SqlAbstractParserImpl abstractParser1 = null;
//        if (Env.USE_VOLCANO_PLANNER)
//        {
//            parser = VolcanoSqlPlanner.create(rootSchema, defaultCatalog, defaultSchema,
//                    new Timeout(Duration.ofSeconds(Env.VOLCANO_PLANNER_TIMEOUT)), quoting);
//        }
//        else
//        {
            parser = HepSqlPlanner.create(rootSchema, defaultSchema, quoting, abstractParser1 );
//                    new Timeout(Duration.ofSeconds(Env.HEP_PLANNER_TIMEOUT)), quoting);
//        }
        return parser;
    }


//    public QueryExecutionNode parse(String sQueryId, String catalogName, String schemaName, String query)
//    {
//        try
//        {
//            SqlNode sqlNode = parseX(query);
//            SqlKind sqlKind = sqlNode.getKind();
//            if (!isDdl(sqlKind))
//            {
//                RelNode optimizedPhase2 = getOptimizedRelNode(sqlNode);
//                System.out.println("Plan is : " + optimizedPhase2.explain());
//            }
//
//        }
//        catch (SqlParseException ex)
//        {
//            throw new RuntimeException(ex.getMessage(), ex);
//        }
//        catch (UnsupportedOperationException ex)
//        {
//            String message = ex.getMessage() == null ? "Unsupported operation in sql" : ex.getMessage();
//            throw new RuntimeException(message, ex);
//        }
//        catch(Throwable ex)
//        {
//            String message = ex.getMessage() == null ? "Error parsing query" : ex.getMessage();
//            throw new RuntimeException(message, ex);
//        }
//    }

//    @Override
//    public RelNode getOptimizedRelNode(SqlNode sqlNode)
//    {
////        HepSqlPlanner planner = new HepSqlPlanner();
//        RelNode converted = planner.convert(sqlNode);
//        RelNode optimized = planner.optimize(converted);
////        QueryCountMetrics.hep();
//        return optimized;
//    }
}
