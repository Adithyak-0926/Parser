package parsing.parsing;

import com.google.common.util.concurrent.Service;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import parsing.SqlStdOperatorTableNew;
import parsing.Swap;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlToRelConverterPlus extends SqlToRelConverter {

protected final RexBuilder rexBuilder;
    private final HintStrategyTable hintStrategies;
    public SqlToRelConverterPlus(RelOptTable.ViewExpander viewExpander, @Nullable SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, SqlToRelConverter.Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
        this.rexBuilder = cluster.getRexBuilder();
        this.hintStrategies = config.getHintStrategyTable();
    }

    @Override
    public RelRoot convertQuery(
            SqlNode query,
            final boolean needsValidation,
            final boolean top) {

            RelRoot result = super.convertQuery(query, needsValidation, top);

            return result.withRel(result.rel.accept(new swappableShuttle()));
//        return result.withRel(result.rel.accept(new swappableShuttlePlus()));


    }


     class swappableShuttle extends RexShuttle{

        @Override public RexNode visitCall(RexCall call){

            SqlOperator operator = call.getOperator();
            if (call.getOperator() == SqlStdOperatorTableNew.LEN || call.getOperator() == SqlStdOperatorTableNew.CHAR_LEN) {
                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);
//                 Replace
                List<RexNode> operands = call.getOperands();
                if(call.getOperator() == SqlStdOperatorTableNew.CHAR_LEN){
//                if(isInSwappables(operator)){
                    List<RexNode> swappedOperands = new ArrayList<>();
                    swappedOperands.add(operands.get(1));
                    swappedOperands.add(operands.get(0));
                    return rexBuilder.makeCall(operator,swappedOperands);
                }
//                when I tried to use rexBuilder directly from constructor, it is showing error saying local variable is not present
                return rexBuilder.makeCall(SqlStdOperatorTableNew.LEN, operands);
            }
            return super.visitCall(call);
        }
     }


     class swappableShuttlePlus extends RexShuttle{
        @Override
        public RexNode visitCall(RexCall call){
            if (isInSwappables(call.getOperator())){
                List<RexNode> operands = call.getOperands();
                List<RexNode> swappedOperands = new ArrayList<>();
                swappedOperands.add(operands.get(1));
                swappedOperands.add(operands.get(0));
               return rexBuilder.makeCall(call.getOperator(),swappedOperands);
            }
            return super.visitCall(call);
        }
     }


    public enum Swappables{
        CharLen
    }

    public boolean isInSwappables(SqlOperator operator) {
        for (Swappables swappable : Swappables.values()) {
            if (swappable.name().equals(operator.getName())) {
                return true;
            }
        }
        return false;
    }
}


