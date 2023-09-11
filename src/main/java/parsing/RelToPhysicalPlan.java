package parsing;

        import parsing.operatormap.OperatorMMap;


//        import com.github.zabetak.calcite.tutorial.rules.CommonIdentifier;
        import com.google.common.collect.ImmutableList;
        import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
        import org.apache.calcite.rel.RelNode;
        import org.apache.calcite.rel.logical.*;
        import org.apache.calcite.rel.type.RelDataTypeFactory;
        import org.apache.calcite.rel.type.RelDataTypeField;
        import org.apache.calcite.rel.type.RelDataTypeSystem;
        import org.apache.calcite.rex.RexCall;
        import org.apache.calcite.rex.RexInputRef;
        import org.apache.calcite.rex.RexLiteral;
        import org.apache.calcite.rex.RexNode;

        import org.apache.calcite.sql.SqlOperator;
        import parsing.parsing.CommonIdentifier;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.List;

public class RelToPhysicalPlan {

    public static AbstractNode getAbstractNode(RelNode relNode, AbstractNode parent) {

        // write some logic to convert rel node to abstract node

        if (relNode instanceof LogicalSort) {

            CustomSortOperator customSortOperator = new CustomSortOperator();

            AbstractNode.E6Field[] fields = getE6Fields(relNode);

            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);

            customSortOperator.setRowOut(rowOut);


            List<RelNode> relNodes = relNode.getInputs();

            customSortOperator.setParent(parent);

            for (RelNode childRelNode : relNodes) {
                customSortOperator.addChild(getAbstractNode(childRelNode, customSortOperator));
            }

            customSortOperator.setRowIn(customSortOperator.getChildren().get(0).getRowOut());

            return customSortOperator;

        } else if (relNode instanceof LogicalJoin) {

            CustomJoinOperator customJoinOperator = new CustomJoinOperator();

            AbstractNode.E6Field[] fields = getE6Fields(relNode);
            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);
            customJoinOperator.setRowOut(rowOut);

            List<RelNode> relNodes = relNode.getInputs();

            customJoinOperator.setParent(parent);


            for (RelNode childRelNode : relNodes) {
                customJoinOperator.addChild(getAbstractNode(childRelNode, customJoinOperator));
            }
            AbstractNode.RowPlus left = customJoinOperator.getChildren().get(0).getRowOut();
            AbstractNode.RowPlus right = customJoinOperator.getChildren().get(1).getRowOut();

            customJoinOperator.setLeft(left);
            customJoinOperator.setRight(right);

            AbstractNode.E6Field[] joinfields = new AbstractNode.E6Field[left.getFieldCount() + right.getFieldCount()];


            for ( int i = 0; i < left.getFieldCount(); i++) {
                joinfields[i] = left.getFieldAtIndex(i);
            }

            for (int i=left.getFieldCount(); i < left.getFieldCount() + right.getFieldCount(); i++) {
                joinfields[i] = right.getFieldAtIndex(i-left.getFieldCount());
            }


            customJoinOperator.setRowIn(new AbstractNode.RowPlus(joinfields));

            return customJoinOperator;
        } else if (relNode instanceof LogicalTableScan) {

            CustomTableOperator customTableOperator = new CustomTableOperator();


            AbstractNode.E6Field[] fields = getE6Fields(relNode);

            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);

            customTableOperator.setRowOut(rowOut);
            customTableOperator.setRowIn(rowOut);

            List<RelNode> relNodes = relNode.getInputs();

            customTableOperator.setParent(parent);


            for (RelNode childRelNode : relNodes) {
                customTableOperator.addChild(getAbstractNode(childRelNode, customTableOperator));
            }

            return customTableOperator;
        } else if (relNode instanceof LogicalProject) {

            CustomProjectOperator customProjectOperator = new CustomProjectOperator();
            List<RexNode> Node = ((LogicalProject) relNode).getProjects();

            List<ConditionNode> conditionNodeList = new ArrayList<>();

            for (RexNode rexNode:Node) {
                ConditionNode conditionNode = build(rexNode);
                conditionNodeList.add(conditionNode);
            }

            customProjectOperator.setExpressions(conditionNodeList);

//            ExpressionNode expressionNode = buildExpressions(Node);
//
//            customProjectOperator.setExpressions(expressionNode);


            AbstractNode.E6Field[] fields = getE6Fields(relNode);
            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);
            customProjectOperator.setRowOut(rowOut);
            List<RelNode> relNodes = relNode.getInputs();

            customProjectOperator.setParent(parent);

            for (RelNode childRelNode : relNodes) {
                customProjectOperator.addChild(getAbstractNode(childRelNode, customProjectOperator));
            }

            customProjectOperator.setRowIn(customProjectOperator.getChildren().get(0).getRowOut());


            return customProjectOperator;
        } else if (relNode instanceof LogicalFilter) {

            CustomFilterOperator customFilterOperator = new CustomFilterOperator();

            AbstractNode.E6Field[] fields = getE6Fields(relNode);
            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);
            customFilterOperator.setRowOut(rowOut);


            RexNode condition = ((LogicalFilter) relNode).getCondition();


            ConditionNode conditionNode = build(condition);


            customFilterOperator.setCondition(conditionNode);


            List<RelNode> relNodes = relNode.getInputs();

            customFilterOperator.setParent(parent);

            for (RelNode childRelNode : relNodes) {
                customFilterOperator.addChild(getAbstractNode(childRelNode, customFilterOperator));
            }

            customFilterOperator.setRowIn(customFilterOperator.getChildren().get(0).getRowOut());

            return customFilterOperator;
        }
         else if (relNode instanceof LogicalValues){
            CustomValueOperator customValueOperator = new CustomValueOperator();

            AbstractNode.E6Field[] fields = getE6Fields(relNode);
            AbstractNode.RowPlus rowOut = new AbstractNode.RowPlus(fields);
            customValueOperator.setRowOut(rowOut);



            ImmutableList<ImmutableList<RexLiteral>> tuples = ((LogicalValues) relNode).getTuples();

            TuplesNode constTuples = buildTuples(tuples);

            customValueOperator.setTuples(constTuples);

            List<RelNode> relNodes = relNode.getInputs();

            customValueOperator.setParent(parent);


            for (RelNode childRelNode : relNodes) {
                customValueOperator.addChild(getAbstractNode(childRelNode, customValueOperator));
            }

//            customValueOperator.setRowIn(customValueOperator.getChildren().get(0).getRowOut());


            return customValueOperator;



        }

        else {
            throw new UnsupportedOperationException();
        }

    }


    public static ConditionNode build(RexNode rexNode) {

        if (rexNode instanceof RexCall) {

            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);


//            RelDataTypeFactory typeFactory = new RelDataTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

//            RelDataTypeFactory typeFactory =(RelDataTypeFactory) TypeFactory.defaultInstance();

            Swap sw = new Swap(typeFactory);

            ConditionNode conditionNode = new ConditionNode();


            SqlOperator conditionOp = ((RexCall) rexNode).getOperator();

            String conditionOperator = (((RexCall) rexNode).getOperator()).getName();

            List<RexNode> operandList = ((RexCall) rexNode).getOperands();

            if(sw.isInSwappables(conditionOp)){

                rexNode= sw.swap(conditionOp,operandList);
            }

            CommonIdentifier.getPopulated();

            HashMap<String,String> opMap = OperatorMMap.Operator_Map;

            if(opMap.containsKey(conditionOperator)){
                conditionOperator = opMap.get(conditionOperator);
            }

            conditionNode.setOperator(conditionOperator);

            List<RexNode> rexNodes = ((RexCall) rexNode).getOperands();

            for (RexNode ChildRexNodes : rexNodes) {
                conditionNode.addOperands(build(ChildRexNodes));
            }
            return conditionNode;

        } else if (rexNode instanceof RexInputRef) {

            String colName = ((RexInputRef) rexNode).getName();

            return new ColsNode(colName);
        } else if (rexNode instanceof RexLiteral) {

            Object value = ((RexLiteral) rexNode).getValue();

            return new ConstNode(value);
        } else {
            throw new UnsupportedOperationException();
        }

    }

    public static TuplesNode buildTuples(ImmutableList<ImmutableList<RexLiteral>> tuples){


        TuplesNode tupleNode = new TuplesNode();


        for (List<RexLiteral> rexTupleList : tuples) {
            List<Object> row = new ArrayList<>();
            for (RexLiteral rexLiteral : rexTupleList) {
                row.add(rexLiteral.getValue());
            }
            tupleNode.addTupleList(row);

        }

        return tupleNode;

    }


//    public static ExpressionNode buildExpressions(List<RexNode> exps){
//
//        ExpressionNode expressionNode = new ExpressionNode();
//
//        for (RexNode rexInput : exps){
//            expressionNode.addExpressions(((RexInputRef)rexInput).getName());
//        }
//        return expressionNode;
//    }

    private static AbstractNode.E6Field[] getE6Fields(RelNode relNode) {
        List<RelDataTypeField> fieldlist = relNode.getRowType().getFieldList();
        AbstractNode.E6Field[] fields = new AbstractNode.E6Field[fieldlist.size()];
        for (int i = 0; i < fieldlist.size(); i++) {
            RelDataTypeField relField = fieldlist.get(i);
            fields[i] = new AbstractNode.E6Field(relField.getName(), relField.getType().getFullTypeString());
        }
        return fields;
    }
}



//     ALTERNATE FOR INSTANCE OF REXCALL- UNDERSTAND WHY AND HOW?

//            SqlBinaryOperator conditionOperator = (SqlBinaryOperator) ((RexCall) rexNode).getOperator();
//            String operatorname = conditionOperator.getName();
//
//            List<ConditionNode> conditionOperands = new ArrayList<>();
//
//            List<RexNode> rexNodes = ((RexCall) rexNode).getOperands();
//
//            for (RexNode childRexNodes : rexNodes) {
//                ConditionNode conditionNode = build(childRexNodes);
//                conditionOperands.add(conditionNode);
//            }
//
//            ConditionNode node = new ConditionNode();
//            node.addOperator(operatorname);
//            node.m_operands.addAll(conditionOperands);
//
//            return node;



//Previous implementation

//        for(int i=0; i<tuples.size(); i++){
//            ImmutableList<RexLiteral> rexTupleList = tuples.get(i);
//
//            List<RexLiteral> tempList = new ArrayList<>();
//
//            for(int j=0; j<rexTupleList.size(); j++){
//                RexLiteral rexTupleLeaf = rexTupleList.get(j);
////                tupleNode.addInnerTupleList(rexTupleLeaf);
//                tempList.add(rexTupleLeaf);
//            }

//
//            tupleNode.addTupleList(rexTupleList);
