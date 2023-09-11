package parsing.parsing;

import com.google.common.util.concurrent.Service;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import parsing.Swap;

import java.security.PublicKey;
import java.util.List;

public class SqlToRelConverterPlus extends SqlToRelConverter {
    public SqlToRelConverterPlus(RelOptTable.ViewExpander viewExpander, @Nullable SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);

        class convertSwappables {
            public RexNode convertCall( SqlNode node) {
                RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                RexBuilder rexBuilder = new RexBuilder(typeFactory);
//      RexShuttle rexShuttle = new RexShuttle();

                RexNode rex = convertExpression(node);
                if(rex instanceof RexCall) {
//        RexNode rexNode = rexShuttle.visitCall((RexCall) rex);
                    if (isInSwappables(((RexCall) rex).getOperator())) {
                        List<RexNode> actualExprs = ((RexCall) rex).getOperands();

                        return rexBuilder.makeCall((SqlOperator) rex.getType(), actualExprs.get(1), actualExprs.get(0));
                    }
                }
                return rex;
            }
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


