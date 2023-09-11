package parsing;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.Collections;
import java.util.List;

public class RexShuttleNew extends RexShuttle {

    private final RexBuilder rexBuilder;
    protected RexShuttleNew(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }
    @Override public RexNode visitCall(final RexCall call) {

        if (call.getOperator() == SqlStdOperatorTableNew.LEN || call.getOperator() == SqlStdOperatorTableNew.CHAR_LEN) {
            // Replace
            List<RexNode> operands = call.getOperands();
            if(operands == SqlStdOperatorTableNew.CHAR_LEN){
                Collections.swap(operands, 0, 1);
            }
            return rexBuilder.makeCall(SqlStdOperatorTableNew.LEN, operands);
        }
        // Return unchaged
        return call;
    }

}



