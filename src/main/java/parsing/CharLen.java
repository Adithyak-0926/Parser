package parsing;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;


public class CharLen extends SqlFunction {
    public CharLen() {
        super("CharLen", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER_NULLABLE, null, OperandTypesPlus.INTEGER_STRING, SqlFunctionCategory.NUMERIC);
    }





}