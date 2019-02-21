package org.chronotics.pithos.ext.es.util;

import org.chronotics.pithos.ext.es.model.ESFieldModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class ESConverterUtil {
    public static HashMap<String, Object> convertMapToMapType(HashMap<String, Object> mapOriginal, List<ESFieldModel> lstField) {
        HashMap<String, Object> mapNew = new HashMap<>();

        for (Map.Entry<String, Object> item : mapOriginal.entrySet()) {
            String strField = item.getKey();
            String strNewField = strField.replace(".", "-");
            Optional<String> optFieldType = lstField.stream().filter(objField -> objField.getFull_name().equals(strNewField)).map(objField -> objField.getType()).findFirst();

            if (optFieldType.isPresent()) {
                String strFieldType = optFieldType.get();
                if (item.getValue() != null) {
                    String strValue = item.getValue().toString();
                    Object objNewValue = null;

                    try {
                        switch (strFieldType) {
                            case ESFilterOperationConstant.DATA_TYPE_TEXT:
                                objNewValue = strValue;
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_DATE:
                                objNewValue = item.getValue();
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_BOOLEAN:
                                objNewValue = Boolean.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_DOUBLE:
                                objNewValue = Double.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_LONG:
                                objNewValue = Long.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_BYTE:
                                objNewValue = Byte.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_FLOAT:
                                objNewValue = Float.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_INTEGER:
                                objNewValue = Integer.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_NUMERIC:
                                objNewValue = Double.valueOf(strValue);
                                break;
                            case ESFilterOperationConstant.DATA_TYPE_SHORT:
                                objNewValue = Short.valueOf(strValue);
                                break;
                            default:
                                objNewValue = item.getValue();
                                break;
                        }
                    } catch (Exception objEx) {
                        objNewValue = item.getValue();
                    }

                    mapNew.put(strNewField, objNewValue);
                } else {
                    mapNew.put(strNewField, null);
                }
            } else {
                mapNew.put(strNewField, item.getValue());
            }
        }

        return mapNew;
    }

    public static <T> Predicate<T> distinctByKey(Function<? super T,Object> keyExtractor) {
        Map<Object,String> seen = new ConcurrentHashMap<>();
        return t -> seen.put(keyExtractor.apply(t), "") == null;
    }
}
