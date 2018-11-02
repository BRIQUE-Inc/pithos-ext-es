package org.chronotics.pithos.ext.es.util;

public class JacksonFilter {
    public static String convertNAString(String strString){
        String strToString = strString.toLowerCase().trim();

        if (strToString.equals("na") || strToString.equals("n/a") || strToString.equals("nan")) {
            return "";
        } else {
            return strString;
        }
    }

    public static Boolean checkNAString(String strString){
        String strToString = strString.toLowerCase().trim();

        if (strToString.equals("na") || strToString.equals("n/a") || strToString.equals("nan")) {
            return true;
        } else {
            return false;
        }
    }
}
