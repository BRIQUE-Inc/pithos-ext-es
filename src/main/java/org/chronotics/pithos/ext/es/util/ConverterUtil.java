package org.chronotics.pithos.ext.es.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

public class ConverterUtil {
    public static String convertDateToString(Date objDate, String strFormat) {
        SimpleDateFormat objSimpleDateFormat = new SimpleDateFormat(strFormat);
        return objSimpleDateFormat.format(objDate);
    }

    public static Long convertDateStringToMillis(String strDate, String strFormat) {
        Long lMillis = 0L;

        try {
            SimpleDateFormat objSimpleDateFormat = new SimpleDateFormat(strFormat);
            Date objDate = objSimpleDateFormat.parse(strDate);
            Calendar objCalender = Calendar.getInstance();
            objCalender.setTime(objDate);
            lMillis = objCalender.getTimeInMillis();
        } catch (Exception objEx) {
        }

        return lMillis;
    }

    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>();

    static {

        DATE_FORMAT_REGEXPS.put("^\\d{8}$", "yyyyMMdd");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "dd/MM/yyyy");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        DATE_FORMAT_REGEXPS.put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
        DATE_FORMAT_REGEXPS.put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}\\.\\d{1,3}$",
                "yyyy-MM-dd HH:mm:ss.S");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd/MM/yyyy HH:mm:ss");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        DATE_FORMAT_REGEXPS.put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
    }

    public static List<String> getDateFormats() {
        return Arrays.asList(DATE_FORMAT_REGEXPS.values().toArray(new String[DATE_FORMAT_REGEXPS.size()]));
    }

    private static Boolean tryBoolean(String strText) {
        try {
            strText = strText.toLowerCase();

            switch (strText) {
                case "true":
                    return true;
                case "false":
                    return false;
                case "1":
                    return true;
                case "0":
                    return false;
                case "y":
                    return true;
                case "n":
                    return false;
                default:
                    return null;
            }
        } catch (Exception objEx) {
            return null;
        }
    }

    private static Date tryDate(String strText) {
        for (Map.Entry<String, String> entry : DATE_FORMAT_REGEXPS.entrySet()) {
            if (strText.toLowerCase().matches(entry.getKey())) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(entry.getValue());
                try {
                    return dateFormat.parse(strText);
                } catch (ParseException e) {
                }
            }
        }
        return null;
    }

    private static Double tryDouble(String strText) {
        try {
            return Double.valueOf(strText);
        } catch (Exception objEx) {
            return null;
        }
    }

    private static Float tryFloat(String strText) {
        try {
            return Float.valueOf(strText);
        } catch (Exception objEx) {
            return null;
        }
    }

    private static Long tryLong(String strText) {
        try {
            return Long.valueOf(strText);
        } catch (Exception objEx) {
            return null;
        }
    }

    private static Integer tryInteger(String strText) {
        try {
            return Integer.valueOf(strText);
        } catch (Exception objEx) {
            return null;
        }
    }

    private static final List<Function<String, Object>> FUNCTIONS = Arrays.asList(s -> tryDate(s),
            s -> tryDouble(s), s -> tryFloat(s), s -> tryLong(s), s -> tryInteger(s), s -> tryBoolean(s));

    public static Object convertStringToDataType(String strValueAsString) {
        return FUNCTIONS.stream().map(f -> f.apply(strValueAsString)).filter(Objects::nonNull).findFirst().orElse(strValueAsString);
    }
}
