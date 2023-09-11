package parsing;

import java.util.ArrayList;
import java.util.List;

public  class ConditionNode {


    protected String m_operator;

    protected  final List<ConditionNode> m_operands = new ArrayList<>();


    public void setOperator(String operator) {
        m_operator = operator;
    }

    public void addOperands(ConditionNode child){
        m_operands.add(child);
    }

//    public void addOperator(String operatorName){
//        m_operator = operatorName;
//    };
//
//    public String getConditionString() {
//        return m_operator;
//    }

    public List<ConditionNode> getOperands() {
        return m_operands;
    }




}