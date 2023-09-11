package parsing;


import java.util.EnumSet;

public enum PoolsOfFunc {

    ADD,

    PLUS,

    MINUS,

    SUB;
    public static final EnumSet<PoolsOfFunc> SUBSTRACTION =
            EnumSet.of(MINUS, SUB);

    public static final EnumSet<PoolsOfFunc> ADDITION =
            EnumSet.of(ADD, PLUS);

}

