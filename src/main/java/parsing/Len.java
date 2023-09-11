package parsing;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

public class Len extends SqlFunction {
    public Len() {
        super("Len", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER_NULLABLE, null,OperandTypesPlus.STRING_INTEGER, SqlFunctionCategory.NUMERIC);
    }



}