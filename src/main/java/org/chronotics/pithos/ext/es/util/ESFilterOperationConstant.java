package org.chronotics.pithos.ext.es.util;

public class ESFilterOperationConstant {
    public static final int IS = 1;
    public static final int IS_NOT = 2;
    public static final int IS_ONE_OF = 3;
    public static final int IS_NOT_ONE_OF = 4;
    public static final int IS_BETWEEN = 5;
    public static final int IS_NOT_BETWEEN = 6;
    public static final int EXISTS = 7;
    public static final int DOES_NOT_EXIST = 8;
    public static final int CORRELATION = 9;
    public static final int COVARIANCE = 10;

    public static final String FILTER_OUTLIER_MILD = "FILTER_OUTLIER_MILD";
    public static final String FILTER_OUTLIER_EXTREME = "FILTER_OUTLIER_EXTREME";
    public static final String FILTER_UCL = "FILTER_UCL";
    public static final String FILTER_LCL = "FILTER_LCL";
    public static final String FILTER_LCL_UCL = "FILTER_LCL_UCL";

    public static final String DATA_TYPE_BOOLEAN = "boolean";
    public static final String DATA_TYPE_TEXT = "keyword";
    public static final String DATA_TYPE_DATE = "date";
    public static final String DATA_TYPE_NUMERIC = "numeric";
    public static final String DATA_TYPE_INTEGER = "integer";
    public static final String DATA_TYPE_FLOAT = "float";
    public static final String DATA_TYPE_DOUBLE = "double";
    public static final String DATA_TYPE_SHORT = "short";
    public static final String DATA_TYPE_BYTE = "byte";
    public static final String DATA_TYPE_LONG = "long";

    public static final String DATA_FORMAT_UPPERCASE = "UpperCase";
    public static final String DATA_FORMAT_LOWERCASE = "LowerCase";
    public static final String DATA_FORMAT_ADD_PREFIX = "Add Prefix";
    public static final String DATA_FORMAT_ADD_POSTFIX = "Add Postfix";
    public static final String DATA_REPLACE_REMOVE_WHITE_SPACE = "Remove White Space";
    public static final String DATA_REPLACE_REMOVE_CHAR = "Remove Char";
    public static final String DATA_REPLACE_REPLACE_TEXT = "Replace Text";
    public static final String DATA_REPLACE_REPLACE_POS =  "Replace Text Range";
    public static final String DATA_REPLACE_REMOVE_NULLITY = "Replace NULLITY";
    public static final String DATA_REPLACE_REMOVE_MISMATCH = "Replace MISTMATCH";

    public static final String KEYWORD_NULLITY = "NULLITY";
    public static final String KEYWORD_MISMATCH = "MISMATCH";

    public static final String PREP_OP_TYPE_DOC = "DOCS";
    public static final String PREP_OP_TYPE_FIELDS = "FIELDS";
    public static final String PREP_OP_TYPE_DATA_CHANGE = "DATA_TYPE_CHANGE";
    public static final String PREP_OP_TYPE_DATA_FORMAT = "DATA_FORMAT";
    public static final String PREP_OP_TYPE_DATA_REPLACE = "DATA_REPLACE";
    public static final String PREP_OP_TYPE_SEPARATOR = "SEPARATOR";

    public static final String PREP_OP_FIELD_ADD = "ADD_FIELD";
    public static final String PREP_OP_FIELD_REMOVE = "REMOVE_FIELD";
    public static final String PREP_OP_DOC_ADD = "ADD_DOC";
    public static final String PREP_OP_DOC_REMOVE = "REMOVE_DOC";

    public static final String PREP_OP_CATEGORY_CHANGE_TO = "change_to";
    public static final String PREP_OP_CATEGORY_ROW_COL = "row_col";
    public static final String PREP_OP_CATEGORY_NAME_CHANGE_TO = "Change to";
    public static final String PREP_OP_CATEGORY_NAME_ROW_COL = "Row/Column";


    public static final String PREP_OP_TYPE_FUNCTION = "FUNCTION";
    public static final String PREP_OP_CATEGORY_FUNCTION = "function";
    public static final String PREP_OP_CATEGORY_NAME_FUNCTION = "Function";

    public static final String FUNCTION_ARITHMETIC_ADD = "Add";
    public static final String FUNCTION_ARITHMETIC_SUB = "Subtract";
    public static final String FUNCTION_ARITHMETIC_MULTIPLY = "Multiply";
    public static final String FUNCTION_ARITHMETIC_DIVIDE = "Divide";
    public static final String FUNCTION_ARITHMETIC_SIN = "Sin";
    public static final String FUNCTION_ARITHMETIC_COS = "Cos";
    public static final String FUNCTION_ARITHMETIC_TAN = "Tan";
    public static final String FUNCTION_ARITHMETIC_LOG = "Log";
    public static final String FUNCTION_ARITHMETIC_LOG10 = "Log10";

    public static final String FUNCTION_STATISTICS_MEAN = "Mean";
    public static final String FUNCTION_STATISTICS_MEDIAN = "Median";
    public static final String FUNCTION_STATISTICS_MIN = "Min";
    public static final String FUNCTION_STATISTICS_MAX = "Max";
    public static final String FUNCTION_STATISTICS_SUM = "Sum";
    public static final String FUNCTION_STATISTICS_VARIANCE = "Variance";
    public static final String FUNCTION_STATISTICS_STD = "StandardDeviation";
    public static final String FUNCTION_STATISTICS_RANGE = "Range";
    public static final String FUNCTION_STATISTICS_STANDARD = "Standardization";
    public static final String FUNCTION_STATISTICS_NORM = "Normalization";

    public static final String FUNCTION_SAMPLING_SIMPLE = "Simple";
    public static final String FUNCTION_SAMPLING_SYSTEMATIC = "Systematic";
    public static final String FUNCTION_SAMPLING_DIST_GAUSSIAN = "Gaussian_Distribution";
    public static final String FUNCTION_SAMPLING_DIST_CONTINUOUS = "Continuous_Uniform_Distribution";
    public static final String FUNCTION_SAMPLING_SMOTE = "SMOTE";

    public static final String FUNCTION_TRANSFORM_LAPLACE = "Laplace";
    public static final String FUNCTION_TRANSFORM_FOURIER = "Fourier";
    public static final String FUNCTION_TRANSFORM_WAVELET = "Wavelet";
    public static final String FUNCTION_TRANSFORM_Z = "Z";
}
