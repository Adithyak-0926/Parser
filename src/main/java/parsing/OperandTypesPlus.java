package parsing;

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;

public class OperandTypesPlus  {

    private OperandTypesPlus() {}
    public static final SqlSingleOperandTypeChecker DATETIME_STRING =
                family(SqlTypeFamily.DATETIME, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker STRING_DATETIME =
            family(SqlTypeFamily.STRING, SqlTypeFamily.DATETIME);

    public static final SqlSingleOperandTypeChecker INTEGER_STRING =
            family(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING);

    public static final SqlSingleOperandTypeChecker STRING_INTEGER =
            family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);





}
