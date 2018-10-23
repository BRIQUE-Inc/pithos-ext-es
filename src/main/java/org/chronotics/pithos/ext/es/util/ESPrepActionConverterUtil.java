package org.chronotics.pithos.ext.es.util;

import org.chronotics.pithos.ext.es.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ESPrepActionConverterUtil {
    //private static Logger objLogger = LoggerFactory.getLogger(ESPrepActionConverterUtil.class);

    public static List<ESPrepAbstractModel> convert(ESPrepListActionRequestModel objAllRequest) {
        List<ESPrepAbstractModel> lstESPrepAbstractModel = new ArrayList<>();

        if (objAllRequest != null && objAllRequest.getActions() != null && objAllRequest.getActions().size() > 0) {
            List<ESPrepActionRequestModel> lstAllActions = objAllRequest.getActions().stream().sorted(Comparator.comparing(ESPrepActionRequestModel::getAction_idx)).collect(Collectors.toList());

            for (ESPrepActionRequestModel objRequest : lstAllActions) {
                ESPrepAbstractModel objPrepAction = null;

                switch (objRequest.getAction_type()) {
                    case ESFilterOperationConstant.PREP_OP_TYPE_DOC:
                        objPrepAction = convertToDataDoc(objRequest);
                        break;
                    case ESFilterOperationConstant.PREP_OP_TYPE_FIELDS:
                        objPrepAction = convertToDataField(objRequest);
                        break;
                    case ESFilterOperationConstant.PREP_OP_TYPE_DATA_CHANGE:
                        objPrepAction = convertToDataTypeChangeType(objRequest);
                        break;
                    case ESFilterOperationConstant.PREP_OP_TYPE_DATA_FORMAT:
                        objPrepAction = convertToDataFormatType(objRequest);
                        break;
                    case ESFilterOperationConstant.PREP_OP_TYPE_DATA_REPLACE:
                        objPrepAction = convertToDataFormatType(objRequest);
                        break;
                }

                if (objPrepAction != null) {
                    objPrepAction.setIndex(objAllRequest.getIndex());
                    objPrepAction.setType(objAllRequest.getType());

                    lstESPrepAbstractModel.add(objPrepAction);
                }
            }
        }

        return lstESPrepAbstractModel;
    }

    private static List<String> getBasicInfoFromRequest(ESPrepActionRequestModel objRequest) {
        String strField = (objRequest.getData_values() != null && objRequest.getData_values().size() > 0) ? objRequest.getData_values().get(0) : "";
        String strInput0 = "";
        String strInput1 = "";
        String strInput2 = "";

        if (strField != null && !strField.isEmpty()) {
            if (objRequest.getUser_values() != null && objRequest.getUser_values().size() > 0) {
                for (int intCount = 0; intCount < objRequest.getUser_values().size(); intCount++) {
                    String strValue = objRequest.getUser_values().get(intCount).getInput_value();

                    switch (objRequest.getUser_values().get(intCount).getInput_idx()) {
                        case 0:
                            strInput0 = strValue;
                            break;
                        case 1:
                            strInput1 = strValue;
                            break;
                        case 2:
                            strInput2 = strValue;
                            break;
                    }
                }
            }
        }

        return Arrays.asList(strField, strInput0, strInput1, strInput2);
    }

    private static ESPrepFormatModel convertToDataFormatType(ESPrepActionRequestModel objRequest) {
        ESPrepFormatModel objPrepFormatModel = new ESPrepFormatModel();
        List<String> lstBasicInfo = getBasicInfoFromRequest(objRequest);
        String strField = lstBasicInfo.get(0);
        String strInput0 = lstBasicInfo.get(1);
        String strInput1 = lstBasicInfo.get(2);
        String strInput2 = lstBasicInfo.get(3);

        if (strField != null && !strField.isEmpty()) {
            objPrepFormatModel.setField(strField);
            objPrepFormatModel.setFormat_op(objRequest.getAction_id());

            switch (objRequest.getAction_id()) {
                case ESFilterOperationConstant.DATA_FORMAT_UPPERCASE:
                    break;
                case ESFilterOperationConstant.DATA_FORMAT_LOWERCASE:
                    break;
                case ESFilterOperationConstant.DATA_FORMAT_ADD_PREFIX:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    break;
                case ESFilterOperationConstant.DATA_FORMAT_ADD_POSTFIX:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    break;
                case ESFilterOperationConstant.DATA_REPLACE_REMOVE_MISMATCH:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    break;
                case ESFilterOperationConstant.DATA_REPLACE_REMOVE_NULLITY:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    break;
                case ESFilterOperationConstant.DATA_REPLACE_REMOVE_CHAR:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    break;
                case ESFilterOperationConstant.DATA_REPLACE_REMOVE_WHITE_SPACE:
                    break;
                case ESFilterOperationConstant.DATA_REPLACE_REPLACE_TEXT:
                    objPrepFormatModel.setFormat_param_1(strInput1);
                    objPrepFormatModel.setFormat_param_2(strInput2);
                    break;
            }
        } else {
            objPrepFormatModel = null;
        }


        return objPrepFormatModel;
    }

    private static ESPrepDataTypeChangeModel convertToDataTypeChangeType(ESPrepActionRequestModel objRequest) {
        ESPrepDataTypeChangeModel objPrepDataTypeChangeModel = new ESPrepDataTypeChangeModel();
        List<String> lstBasicInfo = getBasicInfoFromRequest(objRequest);

        String strField = lstBasicInfo.get(0);
        String strInput0 = lstBasicInfo.get(1);

        if (strField == null || strField.isEmpty()) {
            objPrepDataTypeChangeModel = null;
        } else {
            objPrepDataTypeChangeModel.setField(strField);
            objPrepDataTypeChangeModel.setIs_forced(true);
            objPrepDataTypeChangeModel.setConverted_data_type(objRequest.getAction_id());
            objPrepDataTypeChangeModel.setFailed_default_value("");

            switch (objRequest.getAction_id()) {
                case ESFilterOperationConstant.DATA_TYPE_DATE:
                    objPrepDataTypeChangeModel.setDate_format(strInput0);
                    break;
            }
        }

        return objPrepDataTypeChangeModel;
    }

    private static ESPrepDocModel convertToDataDoc(ESPrepActionRequestModel objRequest) {
        ESPrepDocModel objPrepDoc = new ESPrepDocModel();

        if (objRequest.getData_values() != null && objRequest.getData_values().size() > 0) {
            switch (objRequest.getAction_id()) {
                case ESFilterOperationConstant.PREP_OP_DOC_REMOVE:
                    objPrepDoc.setRemove_doc_ids(objRequest.getData_values());
                    break;
            }
        } else {
            objPrepDoc = null;
        }

        return objPrepDoc;
    }

    private static ESPrepFieldModel convertToDataField(ESPrepActionRequestModel objRequest) {
        ESPrepFieldModel objPrepField = new ESPrepFieldModel();

        if (objRequest.getData_values() != null && objRequest.getData_values().size() > 0) {
            switch (objRequest.getAction_id()) {
                case ESFilterOperationConstant.PREP_OP_FIELD_REMOVE:
                    objPrepField.setRemove_fields(objRequest.getData_values());
                    break;
            }
        } else {
            objPrepField = null;
        }

        return objPrepField;
    }
}
