package parsing;

public class CustomValueOperator extends AbstractNode{

    protected TuplesNode m_constTuples;

    public void setTuples(TuplesNode constTuples) {
        m_constTuples = constTuples;
    }
}