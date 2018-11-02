package org.chronotics.pithos.ext.es.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.chronotics.pithos.ext.es.log.Logger;
import org.chronotics.pithos.ext.es.log.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CSVUtil {
    private static final Logger log = LoggerFactory.getLogger(CSVUtil.class);
    private static final char DEFAULT_SEPARATOR = ',';

    public static void writeLine(Writer w, List<Object> values) throws IOException {
        writeLine(w, values, DEFAULT_SEPARATOR, ' ');
    }

    public static void writeLine(Writer w, List<Object> values, char separators) throws IOException {
        writeLine(w, values, separators, ' ');
    }

    //https://tools.ietf.org/html/rfc4180
    private static String followCVSformat(String value) {

        String result = value;
        if (result.contains("\"")) {
            result = result.replace("\"", "\"\"");
        }
        return result;

    }

    public static void writeLine(Writer w, List<Object> values, char separators, char customQuote) throws IOException {

        boolean first = true;

        //default customQuote is empty

        if (separators == ' ') {
            separators = DEFAULT_SEPARATOR;
        }

        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            if (!first) {
                sb.append(separators);
            }
            if (customQuote == ' ') {
                sb.append(followCVSformat(value.toString()));
            } else {
                sb.append(customQuote).append(followCVSformat(value.toString())).append(customQuote);
            }

            first = false;
        }
        sb.append("\n");
        w.append(sb.toString());
    }

    public static List<HashMap<String, Object>> readCSV(String strFilePath) {
        return readCSV(strFilePath, true);
    }

    private static List<HashMap<String, Object>> readCSV(String strFilePath, Boolean bIsIncludeHeader) {
        List<HashMap<String, Object>> lstCSVData = new ArrayList<>();

        try {
            File objCSVFile = new File(strFilePath);

            if (objCSVFile.exists()) {
                CsvMapper objCSVMapper = new CsvMapper();
                CsvSchema objCSVSchema = CsvSchema.emptySchema().withHeader();

                MappingIterator<HashMap<String, Object>> objCSVIterator = objCSVMapper.readerFor(HashMap.class).with(objCSVSchema).readValues(objCSVFile);

                while (objCSVIterator.hasNext()) {
                    lstCSVData.add(objCSVIterator.next());
                }
            } else {
                lstCSVData = null;
                log.warn("WARN: CSV File is not existed: " + strFilePath);
            }
        } catch (Exception objEx) {
            log.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstCSVData;
    }
}
