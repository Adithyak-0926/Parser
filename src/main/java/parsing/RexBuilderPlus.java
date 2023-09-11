package parsing;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;

import java.util.Collections;
import java.util.List;

public class RexBuilderPlus extends RexBuilder {
    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */

    public RexCall call;
    public RexBuilder rexBuilder;

    public RexBuilderPlus(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    @Override
    public final RexNode makeCall(
            SqlOperator op,
            List<? extends RexNode> exprs
    ) {

        if (call.getOperator() == SqlStdOperatorTableNew.CHAR_LEN) {
            // Replace
            List<RexNode> operands = call.getOperands();
            if (operands == SqlStdOperatorTableNew.CHAR_LEN) {
                Collections.swap(operands, 0, 1);
            }
            return rexBuilder.makeCall(SqlStdOperatorTableNew.LEN, operands);
        }
        return rexBuilder.makeCall(op, exprs);

    }
}
