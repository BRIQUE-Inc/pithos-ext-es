package org.chronotics.pithos.ext.es.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
    public static String getStrackTrace(Throwable objEx) {
        StringWriter objSW = new StringWriter();
        PrintWriter objPW = new PrintWriter(objSW);
        objEx.printStackTrace(objPW);

        String strStackTrace = objSW.toString();

        return strStackTrace;
    }
}
