package parsing;

import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;

import java.util.List;


public class Swap {

    protected RelDataTypeFactory typeFactory;
    RexBuilder rexBuilder;


    public Swap(RelDataTypeFactory typeFactory){
        this.typeFactory = typeFactory;
        this.rexBuilder = new RexBuilder(typeFactory);
    }

//    RexBuilder rexBuilder = new RexBuilder(typefactory);
//
//    public void setRexBuilder(RexBuilder rexBuilder) {
//        this.rexBuilder = rexBuilder;
//    }



    public RexNode swap(SqlOperator operator, List<RexNode> OperandsList){
        RexNode leftOperand = OperandsList.get(0);
        RexNode RightOperand= OperandsList.get(1);
        RexNode rexNode1 = rexBuilder.makeCall(operator,RightOperand, leftOperand);
        return rexNode1;
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
