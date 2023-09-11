package parsing;



import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.List;

public  class TuplesNode {

    protected final List<List<Object>> m_TupleList = new ArrayList<>();

//    protected final List<RexLiteral> m_InnerTupleList = new ArrayList<>();

    public void addTupleList(List<Object> m_STupleList) {
        m_TupleList.add(m_STupleList);
    }

//    public void addInnerTupleList(RexLiteral child) {
//        m_InnerTupleList.add(child);
//    }


}

