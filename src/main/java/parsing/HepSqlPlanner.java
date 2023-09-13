package parsing;//package io.e6x.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
//import jdk.internal.org.jline.utils.Timeout;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;
import org.jetbrains.annotations.NotNull;
import parsing.parsing.SchemaCustom;
import parsing.parsing.SqlToRelConverterPlus;

import javax.annotation.Nullable;
import java.util.*;
//import org.apache.commons.lang3.tuple.*;

//import static io.e6x.engine.service.Env.SQL_CONFORMANCE_ENUM;
//import static io.e6x.sql.planner.optimizer.rules.E6Rules.EXPRESSION_REWRITE_RULE;

public class HepSqlPlanner extends SqlParser1
{

    public static final ImmutableSet<RelOptRule> RULE_SET = ImmutableSet.of(CoreRules.AGGREGATE_MERGE,
            CoreRules.AGGREGATE_REMOVE, CoreRules.AGGREGATE_JOIN_TRANSPOSE,
            CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED, CoreRules.CALC_MERGE, CoreRules.CALC_REMOVE,
            CoreRules.CALC_REDUCE_DECIMALS, CoreRules.CALC_SPLIT, CoreRules.CALC_TO_WINDOW, CoreRules.COERCE_INPUTS,
            CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS, CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS, CoreRules.FILTER_INTO_JOIN,
            CoreRules.FILTER_MERGE, CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_SCAN, CoreRules.FILTER_CORRELATE, CoreRules.INTERSECT_MERGE, CoreRules.INTERSECT_TO_DISTINCT,
            CoreRules.MINUS_MERGE, CoreRules.PROJECT_JOIN_JOIN_REMOVE, CoreRules.PROJECT_JOIN_REMOVE,
            CoreRules.PROJECT_JOIN_TRANSPOSE, CoreRules.PROJECT_MERGE, CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_TABLE_SCAN, CoreRules.PROJECT_INTERPRETER_TABLE_SCAN,
            CoreRules.JOIN_CONDITION_PUSH, CoreRules.JOIN_PUSH_EXPRESSIONS, CoreRules.SORT_UNION_TRANSPOSE,
            CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH, CoreRules.SORT_REMOVE, CoreRules.SORT_REMOVE_CONSTANT_KEYS,
            CoreRules.UNION_MERGE, CoreRules.UNION_REMOVE, CoreRules.UNION_PULL_UP_CONSTANTS, CoreRules.UNION_TO_DISTINCT,
            CoreRules.AGGREGATE_VALUES, CoreRules.FILTER_VALUES_MERGE, CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE, CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
            // Partition by rules
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW, CoreRules.PROJECT_WINDOW_TRANSPOSE);
    // Temporary workaround: Disabling the subquery rules
// TODO: Fix the cast conversion error + other errors
//CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
//CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
//CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
    private final SqlValidator m_validator;
    private final SqlToRelConverter m_converter;
    private final SqlToRelConverterPlus m_converterPLus;
    private final org.apache.calcite.sql.parser.SqlParser.Config m_parserConfig;

    //    protected static final Logger s_logger = Utility.getLogger();
    private final CalciteSchema m_schema;
    //    private PlanningStats m_planningStats;
    private final ArrayList<Pair<HepProgram, HepPlanner>> m_mapOfHEPPrograms;

    private static RelOptCluster m_cluster = null;

//    public final SqlAbstractParserImpl abstractParser;

    protected HepSqlPlanner(CalciteConnectionConfig config, RelOptCluster cluster, SqlValidator validator,
                            SqlToRelConverter converter, SqlToRelConverterPlus mConverterPLus, ArrayList<Pair<HepProgram, HepPlanner>> mapOfHEPPrograms, CalciteSchema schema,
                            Quoting quoting)
//    SqlAbstractParserImpl abstractParser)
    {
//        super(abstractParser,(SqlParser.Config) config);
        m_validator = validator;
        m_converter =  converter;
        m_converterPLus = mConverterPLus;
//        this.abstractParser = abstractParser;
        m_parserConfig = org.apache.calcite.sql.parser.SqlParser.config()
                .withParserFactory(SqlParserImpl.FACTORY)
                .withCaseSensitive(config.caseSensitive())
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuoting(quoting)
                .withConformance(config.conformance());

        m_cluster = cluster;
        m_mapOfHEPPrograms = mapOfHEPPrograms;
        m_schema = schema;
//        m_planningStats = planningStats;
    }



    public static HepSqlPlanner create(CalciteSchema rootSchema, String defaultSchema,
                                       Quoting quoting, SqlAbstractParserImpl abstractParser)
    {
        rootSchema = rootSchema.root();
        SchemaCustom customSchema = new SchemaCustom();
//        rootSchema.add(schemaCustom.getSchemaName(),schemaCustom);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
//        configProperties.put(CalciteConnectionProperty.CONFORMANCE.camelName(), SQL_CONFORMANCE_ENUM.name());

        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootCalciteSchema = CalciteSchema.createRootSchema(true, false, customSchema.getSchemaName(),
                customSchema);
//        CalciteSchema rootCalciteSchema = CalciteSchema.createRootSchema(false, false, rootSchema.getName(),
//                rootSchema.schema);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootCalciteSchema,
                ImmutableList.of(customSchema.getSchemaName()), typeFactory, config);

        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(ImmutableList.of(SqlStdOperatorTableNew.instance()));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT.withLenientOperatorLookup(
                        config.lenientOperatorLookup())
//                .withSqlConformance(SQL_CONFORMANCE_ENUM)
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true)
                .withTypeCoercionRules(getSqlTypeCoercionRules());

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        // Instrument the query compilation time
        long startQueryCompileTime = System.nanoTime();
        long endQueryCompileTime = System.nanoTime();
        long compileTime = (endQueryCompileTime - startQueryCompileTime) / 1000_000;
//        s_logger.info("Query Compile time: {} ms", compileTime);

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false)
                .withDecorrelationEnabled(true)
                .withInSubQueryThreshold(25);

        ArrayList<Pair<HepProgram, HepPlanner>> mapOfHEPPrograms = new ArrayList<>();
        HepProgram programMain1 = new HepProgramBuilder()
                .addRuleCollection(RULE_SET)
                .build();
        mapOfHEPPrograms.add(Pair.of(programMain1, createPlannerPhase(RULE_SET, Integer.MAX_VALUE)));

        // addMatchLimit should have set the rule match limit but it doesn't.
        HepProgram programMain2 = new HepProgramBuilder().addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
                .addRuleCollection(ImmutableList.of(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES))
                .build();
        mapOfHEPPrograms.add(
                Pair.of(programMain2, createPlannerPhase(ImmutableList.of(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES), 10)));
        mapOfHEPPrograms.add(Pair.of(buildHEPProgram(ImmutableList.of(CoreRules.FILTER_PROJECT_TRANSPOSE)),
                createPlannerPhase(ImmutableList.of(CoreRules.FILTER_PROJECT_TRANSPOSE), Integer.MAX_VALUE)));
        mapOfHEPPrograms.add(Pair.of(buildHEPProgram(ImmutableList.of(CoreRules.FILTER_MERGE)),
                createPlannerPhase(ImmutableList.of(CoreRules.FILTER_MERGE), Integer.MAX_VALUE)));

        RelOptCluster cluster = RelOptCluster.create(mapOfHEPPrograms.get(0).right, new RexBuilderPlus(typeFactory));
        //cluster.setMetadataProvider(MetadataProvider.build(rootSchema, defaultCatalog, defaultSchema, null));

        SqlToRelConverter converter = new SqlToRelConverter(NOOP_EXPANDER,validator,catalogReader,cluster,StandardConvertletTable.INSTANCE,converterConfig);

        SqlToRelConverterPlus converterPlus = new SqlToRelConverterPlus(NOOP_EXPANDER,validator,catalogReader,cluster,StandardConvertletTable.INSTANCE,converterConfig);
        return new HepSqlPlanner(config, cluster, validator, converter, converterPlus, mapOfHEPPrograms, rootCalciteSchema,
                 quoting);
    }


    @NotNull
    private static SqlTypeCoercionRule getSqlTypeCoercionRules()
    {
        Map<SqlTypeName, ImmutableSet<SqlTypeName>> map = new HashMap<>();
        map.putAll(SqlTypeCoercionRule.instance().getTypeMapping());
        ImmutableSet<SqlTypeName> typesWhichCanCastToVarchar = map.get(SqlTypeName.VARCHAR);
        HashSet<SqlTypeName> finalTypesWhichCanCastToVarchar = new HashSet<>();
        finalTypesWhichCanCastToVarchar.addAll(typesWhichCanCastToVarchar);
        finalTypesWhichCanCastToVarchar.add(SqlTypeName.ARRAY);
        map.put(SqlTypeName.VARCHAR, ImmutableSet.copyOf(finalTypesWhichCanCastToVarchar));
        SqlTypeCoercionRule sqlTypeCoercionRule = SqlTypeCoercionRule.instance(map);
        return sqlTypeCoercionRule;
    }

    @NotNull
    private static HepPlanner createPlannerPhase(Collection<RelOptRule> ruleSet, @Nullable Integer matchLimit)
    {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleCollection(ruleSet);

        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
        return planner;
    }

    protected RelNode convert(SqlNode node)
    {
        RelRoot root = m_converterPLus.convertQuery(validateNode(node), false, true);

        return root.rel;
    }

    private SqlNode validateNode(SqlNode node)
    {
        return m_validator.validate(node);
    }

    public RelNode optimize(RelNode node)
    {
        RelNode optimizedNode = node;
        while (true)
        {
            Pair<Pair<Boolean, HepProgram>, RelNode> pairRelNodePair = runOptimizePhases(optimizedNode);
            Boolean shouldRerun = pairRelNodePair.left.left;
            if (Boolean.TRUE.equals(shouldRerun))
            {
                HepProgram programToRemove = pairRelNodePair.left.right;
                m_mapOfHEPPrograms.remove(programToRemove);
                continue;
            }
            optimizedNode = pairRelNodePair.right;
            break;
        }
//        m_planningStats.planType(PlanningStats.PlanType.HEP);
        return optimizedNode;
    }

    @Override
    public RelOptPlanner getPlanner()
    {
        return null;
    }

    @Override
    public RelMetadataQuery getRelMetaDataQuery()
    {
        return null;
    }

//    @Override
//    public PlanningStats getPlanningStats()
//    {
//        return m_planningStats;
//    }

    private Pair<Pair<Boolean, HepProgram>, RelNode> runOptimizePhases(RelNode optimizedNode)
    {
        HepProgram programToIgnore;
        HepProgram plannerProgram = null;
        HepPlanner planner;

        for (int i = 0; i < m_mapOfHEPPrograms.size(); i++)
        {
            try
            {
                plannerProgram = m_mapOfHEPPrograms.get(i).left;
                planner = m_mapOfHEPPrograms.get(i).right;
                RelNode origNode = optimizedNode;
                planner.setRoot(optimizedNode);

                // JOIN_PUSH_TRANSITIVE_PREDICATES resulted in an infinite recursion;
                // hence setting the limit of this rule to 50. Please see HepProgram for
                // further comments.
//                if (i == 1)
//                {
//                    plannerProgram.MATCH_UNTIL_FIXPOINT = 10;
//                }
//                else
//                {
//                    plannerProgram.MATCH_UNTIL_FIXPOINT = Integer.MAX_VALUE;
//                }

                // Timeout has a flaw: it doesn't guarantee the termination of child thread
                // Till we figure out a robust way to handle this, timeout might lead to
                // child threads spinning on CPU. The timeout infrastructure is in place
                // and can be easily turned on once we resolve the child thread issue.
            /* optimizedNode =   m_timeout.call(() ->Programs.of(plannerProgram, true, MetadataProvider.buildProviderForHEP())
                .run(planner, origNode, origNode.getTraitSet(),
                   planner.getMaterializations(), Collections.emptyList())); */
                optimizedNode = Programs.of(plannerProgram, true, m_cluster.getMetadataProvider())
                        .run(planner, origNode, origNode.getTraitSet(), planner.getMaterializations(), Collections.emptyList());
            }
            catch (Exception timeoutException)
            {
                programToIgnore = plannerProgram;
                return Pair.of(Pair.of(true, programToIgnore), null);
            }
        }

        return Pair.of(Pair.of(false, null), optimizedNode);
    }

    @Override
    public RelNode getOptimizedRelNode(SqlNode sqlNode)
    {
        RelNode converted = convert(sqlNode);
        RelNode optimized = optimize(converted);
//        QueryCountMetrics.hep();
        return optimized;
    }

    public static HepProgram buildHEPProgram(Collection<RelOptRule> ruleSet)
    {
        return new HepProgramBuilder().addRuleCollection(ruleSet).build();
    }

    @Override
    protected SqlNode parseX(String query) throws SqlParseException
    {
        return org.apache.calcite.sql.parser.SqlParser.create(query, m_parserConfig).parseStmt();
    }

    protected CalciteSchema getSchema()
    {
        return m_schema;
    }


    public RexBuilder getRexBuilder()
    {
        return m_converter.getRexBuilder();
    }

    public static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

}