package parsing;

import org.apache.calcite.rex.RexInputRef;

import java.util.List;

public class ExpressionNode {

    protected List<String> m_expressions;

    public void addExpressions(String expression){
        m_expressions.add(expression);
    }
}
