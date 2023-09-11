package parsing;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.List;

public class CustomSqlVisitor extends SqlBasicVisitor<SqlNode> {
    @Override
    public SqlNode visit(SqlCall call) {
        if (call.getOperator().equals(SqlStdOperatorTableNew.LEN)) {
            List<SqlNode> operands = call.getOperandList();
            if (operands.size() == 2) {
                return SqlStdOperatorTableNew.LEN.createCall(
                        call.getFunctionQuantifier(),
                        call.getParserPosition(),
                        operands.get(1),
                        operands.get(0)

                );
            }
        }

        List<SqlNode> modifiedOperands = call.getOperandList();
        for (int i = 0; i < modifiedOperands.size(); i++) {
            modifiedOperands.set(i, modifiedOperands.get(i).accept(this));
        }
         return call.getOperator().createCall(
                 call.getFunctionQuantifier(),
                 call.getParserPosition(),
                 modifiedOperands
         );


    }
}
