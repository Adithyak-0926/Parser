package parsing.parsing;

import java.util.HashMap;


import parsing.operatormap.OperatorMMap;

public class CommonIdentifier {
    public static void getPopulated() {
        HashMap<String, String> OperatorMap = OperatorMMap.Operator_Map;

        OperatorMap.put("ADD", "ADDITION");
        OperatorMap.put("SUB", "SUBTRACTION");
        OperatorMap.put("Len", "LOC");
        OperatorMap.put("CharLen", "LOC");
    }

}
