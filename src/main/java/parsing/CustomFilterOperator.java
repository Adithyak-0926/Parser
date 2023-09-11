package parsing;

import org.apache.calcite.rex.RexNode;

public class CustomFilterOperator extends AbstractNode {

    protected ConditionNode m_condition;

//    protected RowPlus m_OP;
    public void setCondition(ConditionNode condition){
             m_condition = condition;
      }
//    public void setOperator(RowPlus operatorType){
//        m_OP = operatorType;
//    }
//    public class FilterCondition {
//        private String operatorType;
//        private String conditionString;
//
//        public FilterCondition(String operatorType, String conditionString) {
//            this.operatorType = operatorType;
//            this.conditionString = conditionString;
//        }
//
//        public String getOperatorType() {
//            return operatorType;
//        }
//
//        public String getConditionString() {
//            return conditionString;
//        }


    }


