package parsing;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractNode {

    protected AbstractNode m_parent;

    protected final List<AbstractNode> m_children = new ArrayList<>();

    protected RowPlus m_rowIn;

    protected RowPlus m_rowOut;




    public void setRowIn(RowPlus rowIn){
        m_rowIn = rowIn;
    }
    public void setRowOut(RowPlus rowOut){
        m_rowOut = rowOut;
    }



    public void addChild(AbstractNode child) {
        m_children.add(child);
    }


    public void setParent(AbstractNode parent) {
        m_parent = parent;
    }


    public AbstractNode getParent() {
        return m_parent;
    }

    public List<AbstractNode> getChildren(){
        return m_children;
    }

    public String getChildrenAsString() {
        StringBuilder sb = new StringBuilder();
        for (AbstractNode child : m_children) {
            sb.append(child.toString()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "AbstractNode";
    }

    public RowPlus getRowOut() {
        return m_rowOut;
    }


    public static class RowPlus {

        E6Field[] m_aFields;

        public RowPlus(E6Field[] fields){
            m_aFields = fields;
        }

        public E6Field getFieldAtIndex(int index) {
            return m_aFields[index];
        }

        public int getFieldCount(){
            return m_aFields.length;
        }
    }

    public static class E6Field {

        private String fieldName;

        private String fieldType;


        public E6Field(String name, String type) {
            fieldName = name;
            fieldType = type;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }
    }





}