package parsing;


import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class SqlStdOperatorTableNew extends SqlStdOperatorTable {

    private static @MonotonicNonNull SqlStdOperatorTableNew instance;

    public static synchronized SqlStdOperatorTable instance() {
        if (instance == null) {
            instance = new SqlStdOperatorTableNew();
            instance.init();
        }

        return instance;
    }

        //Initializing part the arguments we pass are - name:  , Sqlkind.   , ReturnTypes.   , (SqlOperandTypeInterferance)   , OperandTypes.  ,  SqlFunctionCategory.  )

        public static final SqlFunction MOD_CUSTOM;

    public static final SqlFunction DATETIME_CUSTOM ;

    public static final SqlFunction CONVERT_TIMEZONE_CUSTOM;

    public static final SqlFunction ADD_CUSTOM;

    public static final SqlFunction PLUS_CUSTOM;

    public static final SqlFunction MINUS_CUSTOM;

    public static final SqlFunction SUB_CUSTOM;


    static {
            MOD_CUSTOM = new SqlFunction("MOD_CUSTOM", SqlKind.MOD, ReturnTypes.NULLABLE_MOD, (SqlOperandTypeInference) null, OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC, SqlFunctionCategory.NUMERIC);

            DATETIME_CUSTOM = new SqlFunction("DATETIME",SqlKind.OTHER_FUNCTION,ReturnTypes.TIMESTAMP_NULLABLE,(SqlOperandTypeInference) null,OperandTypesPlus.DATETIME_STRING,SqlFunctionCategory.TIMEDATE);

            CONVERT_TIMEZONE_CUSTOM = new SqlFunction("CONVERT_TIMEZONE", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE, (SqlOperandTypeInference) null,
                    OperandTypesPlus.STRING_DATETIME, SqlFunctionCategory.TIMEDATE);

            ADD_CUSTOM = new SqlFunction("ADD", SqlKind.PLUS , ReturnTypes.NULLABLE_SUM,(SqlOperandTypeInference) null,OperandTypes.NUMERIC_NUMERIC , SqlFunctionCategory.NUMERIC);

        PLUS_CUSTOM = new SqlFunction("PLUS", SqlKind.PLUS , ReturnTypes.NULLABLE_SUM,(SqlOperandTypeInference) null,OperandTypes.NUMERIC_NUMERIC , SqlFunctionCategory.NUMERIC);

        MINUS_CUSTOM = new SqlFunction("MINUS", SqlKind.MINUS , ReturnTypes.NULLABLE_SUM,(SqlOperandTypeInference) null,OperandTypes.NUMERIC_NUMERIC , SqlFunctionCategory.NUMERIC);

        SUB_CUSTOM = new SqlFunction("SUB", SqlKind.MINUS , ReturnTypes.NULLABLE_SUM,(SqlOperandTypeInference) null,OperandTypes.NUMERIC_NUMERIC , SqlFunctionCategory.NUMERIC);

    }

    public static final SqlFunction LEN = new Len();

    public static final SqlFunction CHAR_LEN = new CharLen();

}

//public class SqlStdOperatorTablePlus  extends SqlStdOperatorTable {
//
//    private static @MonotonicNonNull SqlStdOperatorTablePlus instance;
//
//
//    /**
//     * Returns the standard operator table, creating it if necessary.
//     */
//    public static synchronized SqlStdOperatorTablePlus instance() {
//        if (instance == null) {
//            // Creates and initializes the standard operator table.
//            // Uses two-phase construction, because we can't initialize the
//            // table until the constructor of the sub-class has completed.
//            instance = new SqlStdOperatorTablePlus();
//            instance.init();
//        }
//        return instance;
//    }
