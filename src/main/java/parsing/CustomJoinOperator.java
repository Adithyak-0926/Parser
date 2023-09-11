package parsing;

import java.util.ArrayList;
import java.util.List;

public class CustomJoinOperator extends AbstractNode {

    private final List<String> joinFields = new ArrayList<>();

    public CustomJoinOperator(List<String> joinFields) {
        this.joinFields.addAll(joinFields);
    }

    public CustomJoinOperator() {
    }

    public List<String> getJoinFields() {
        return joinFields;
    }

    protected RowPlus m_Left;

    protected RowPlus m_Right;


    public void setLeft(RowPlus left){
        m_Left = left;
    }

    public void setRight(RowPlus right){
        m_Right= right;
    }



}



