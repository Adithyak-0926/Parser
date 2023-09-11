package parsing;

import org.apache.calcite.rex.RexCall;

import java.util.List;

public class CustomProjectOperator extends AbstractNode{

    protected List<ConditionNode> m_expns;

    public void setExpressions(List<ConditionNode> expressionList){
        m_expns = expressionList;
    }
}
