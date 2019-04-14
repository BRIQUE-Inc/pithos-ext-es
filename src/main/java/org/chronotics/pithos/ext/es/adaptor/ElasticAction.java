package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.internal.Strings;
import org.chronotics.pandora.java.converter.ConverterUtil;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.file.CSVUtil;
import org.chronotics.pandora.java.file.FileUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.chronotics.pandora.java.math.MathUtil;
import org.chronotics.pithos.ext.es.util.ESPithosConstant;
import org.chronotics.pithos.ext.es.model.*;
import org.chronotics.pithos.ext.es.util.*;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.swing.text.StringContent;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ElasticAction {
    private Logger objLogger = LoggerFactory.getLogger(ElasticAction.class);
    ElasticConnection objESConnection;
    ElasticFilter objESFilter;
    TransportClient objESClient;
    Integer intNumBulkOperation = 20000;
    ObjectMapper objMapper = new ObjectMapper();
    Random objRandom = new Random();

    Long lScrollTTL = 600000L;

    public ElasticAction(ElasticConnection objESConnection, ElasticFilter objESFilter, Integer intNumBulkOperation) {
        this.objESConnection = objESConnection;
        this.objESClient = objESConnection.objESClient;
        this.objESFilter = objESFilter;
        this.intNumBulkOperation = 20000;
    }

    public Map<String, List<ESPrepActionTypeModel>> getPrepActionTypes() {
        Map<String, List<ESPrepActionTypeModel>> mapResult = new HashMap<>();
        List<ESPrepActionTypeModel> lstTextActionTypeModel = new ArrayList<>();
        List<ESPrepActionTypeModel> lstNumberActionTypeModel = new ArrayList<>();
        List<String> lstDateFormat = ConverterUtil.getDateFormats();

        //SEPARATOR
        ESPrepActionTypeModel objSeparateActionTypeModel = new ESPrepActionTypeModel();
        objSeparateActionTypeModel.setId(ESFilterOperationConstant.PREP_OP_TYPE_SEPARATOR);
        objSeparateActionTypeModel.setValue("-");

        //ROWS
        ESPrepActionModel objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.PREP_OP_DOC_REMOVE);
        objPrepAction.setAction_name("Delete row(s)");
        objPrepAction.setIs_show(true);

        ESPrepActionTypeModel objPrepActionType = new ESPrepActionTypeModel();
        objPrepActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_DOC);
        objPrepActionType.setValue("Rows");
        objPrepActionType.setCategory_id(ESFilterOperationConstant.PREP_OP_CATEGORY_ROW_COL);
        objPrepActionType.setCategory_name(ESFilterOperationConstant.PREP_OP_CATEGORY_NAME_ROW_COL);
        objPrepActionType.setActions(Arrays.asList(objPrepAction));

        mapResult.put("rows", Arrays.asList(objPrepActionType));

        //COLUMNS
        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.PREP_OP_FIELD_REMOVE);
        objPrepAction.setAction_name("Delete column(s)");
        objPrepAction.setIs_show(true);

        objPrepActionType = new ESPrepActionTypeModel();
        objPrepActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_FIELDS);
        objPrepActionType.setValue("Columns");
        objPrepActionType.setCategory_id(ESFilterOperationConstant.PREP_OP_CATEGORY_ROW_COL);
        objPrepActionType.setCategory_name(ESFilterOperationConstant.PREP_OP_CATEGORY_NAME_ROW_COL);
        objPrepActionType.setActions(Arrays.asList(objPrepAction));

        mapResult.put("columns", Arrays.asList(objPrepActionType));

        //FUNCTIONAL
        List<ESPrepActionModel> lstFunctionAction = new ArrayList<>();

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_ADD);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_ADD);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_SUB);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_SUB);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_MULTIPLY);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_MULTIPLY);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_DIVIDE);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_DIVIDE);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_SIN);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_SIN);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_COS);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_COS);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_TAN);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_TAN);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG10);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG10);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.FUNCTION_ARITHMETIC_FORMULA);
        objPrepAction.setAction_name(ESFilterOperationConstant.FUNCTION_ARITHMETIC_FORMULA);
        objPrepAction.setIs_show(true);
        lstFunctionAction.add(new ESPrepActionModel(objPrepAction));

        objPrepActionType = new ESPrepActionTypeModel();
        objPrepActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_FUNCTION);
        objPrepActionType.setValue("Functions");
        objPrepActionType.setCategory_id(ESFilterOperationConstant.PREP_OP_CATEGORY_FUNCTION);
        objPrepActionType.setCategory_name(ESFilterOperationConstant.PREP_OP_CATEGORY_NAME_FUNCTION);
        objPrepActionType.setActions(lstFunctionAction);

        mapResult.put(ESFilterOperationConstant.PREP_OP_CATEGORY_FUNCTION, Arrays.asList(objPrepActionType));

        //DATA TYPE CHANGE
        List<ESPrepActionModel> lstDataTypeChangeTextAction = new ArrayList<>();
        List<ESPrepActionModel> lstDataTypeChangeNumberAction = new ArrayList<>();

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_TYPE_TEXT);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_TYPE_TEXT);
        objPrepAction.setIs_show(true);
        objPrepAction.setData_type(ESFilterOperationConstant.DATA_TYPE_TEXT);
        lstDataTypeChangeTextAction.add(new ESPrepActionModel(objPrepAction));
        lstDataTypeChangeNumberAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_TYPE_BOOLEAN);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_TYPE_BOOLEAN);
        objPrepAction.setIs_show(true);
        objPrepAction.setData_type(ESFilterOperationConstant.DATA_TYPE_BOOLEAN);
        lstDataTypeChangeTextAction.add(new ESPrepActionModel(objPrepAction));
        lstDataTypeChangeNumberAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_TYPE_DATE);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_TYPE_DATE);
        objPrepAction.setIs_show(true);
        objPrepAction.setData_type(ESFilterOperationConstant.DATA_TYPE_DATE);
        objPrepAction.setPredefined_values(lstDateFormat);
        lstDataTypeChangeTextAction.add(new ESPrepActionModel(objPrepAction));
        lstDataTypeChangeNumberAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_TYPE_DOUBLE);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_TYPE_DOUBLE);
        objPrepAction.setIs_show(true);
        objPrepAction.setData_type(ESFilterOperationConstant.DATA_TYPE_DOUBLE);
        lstDataTypeChangeTextAction.add(new ESPrepActionModel(objPrepAction));
        lstDataTypeChangeNumberAction.add(new ESPrepActionModel(objPrepAction));

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_TYPE_LONG);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_TYPE_LONG);
        objPrepAction.setIs_show(true);
        objPrepAction.setData_type(ESFilterOperationConstant.DATA_TYPE_LONG);
        lstDataTypeChangeTextAction.add(new ESPrepActionModel(objPrepAction));
        lstDataTypeChangeNumberAction.add(new ESPrepActionModel(objPrepAction));

        ESPrepActionTypeModel objNumberDataTypeActionType = new ESPrepActionTypeModel();
        ESPrepActionTypeModel objTextDataTypeActionType = new ESPrepActionTypeModel();

        objNumberDataTypeActionType.setActions(lstDataTypeChangeNumberAction);
        objNumberDataTypeActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_DATA_CHANGE);
        objNumberDataTypeActionType.setValue("Change type");

        objTextDataTypeActionType.setActions(lstDataTypeChangeTextAction);
        objTextDataTypeActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_DATA_CHANGE);
        objTextDataTypeActionType.setValue("Change type");

        objNumberDataTypeActionType.getActions().get(0).setIs_show(true);
        objNumberDataTypeActionType.getActions().get(1).setIs_show(false);
        objNumberDataTypeActionType.getActions().get(2).setIs_show(false);
        objNumberDataTypeActionType.getActions().get(3).setIs_show(false);
        objNumberDataTypeActionType.getActions().get(4).setIs_show(false);

        objTextDataTypeActionType.getActions().get(0).setIs_show(false);
        objTextDataTypeActionType.getActions().get(1).setIs_show(true);
        objTextDataTypeActionType.getActions().get(2).setIs_show(true);
        objTextDataTypeActionType.getActions().get(3).setIs_show(true);
        objTextDataTypeActionType.getActions().get(4).setIs_show(true);

        //DATA FORMAT
        List<ESPrepActionModel> lstDataFormatGeneralAction = new ArrayList<>();

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_FORMAT_UPPERCASE);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_FORMAT_UPPERCASE);
        objPrepAction.setIs_show(true);
        lstDataFormatGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_FORMAT_LOWERCASE);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_FORMAT_LOWERCASE);
        objPrepAction.setIs_show(true);
        lstDataFormatGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_FORMAT_ADD_PREFIX);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_FORMAT_ADD_PREFIX);
        objPrepAction.setIs_show(true);

        ESFilterCustomModel objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(1);
        ESFilterTextDefaultModel objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Prefix");
        objCustomValue.setUi(Arrays.asList(objCustomTextValue));

        objPrepAction.setCustom_value(objCustomValue);

        lstDataFormatGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_FORMAT_ADD_POSTFIX);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_FORMAT_ADD_POSTFIX);
        objPrepAction.setIs_show(true);

        objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(1);
        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Postfix");
        objCustomValue.setUi(Arrays.asList(objCustomTextValue));

        objPrepAction.setCustom_value(objCustomValue);

        lstDataFormatGeneralAction.add(objPrepAction);

        ESPrepActionTypeModel objDataFormatActionType = new ESPrepActionTypeModel();
        objDataFormatActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_DATA_FORMAT);
        objDataFormatActionType.setValue("Format");
        objDataFormatActionType.setActions(lstDataFormatGeneralAction);

        //DATA REPLACE
        List<ESPrepActionModel> lstDataReplaceGeneralAction = new ArrayList<>();

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_REPLACE_REMOVE_WHITE_SPACE);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_REPLACE_REMOVE_WHITE_SPACE);
        objPrepAction.setIs_show(true);
        lstDataReplaceGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_REPLACE_REMOVE_CHAR);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_REPLACE_REMOVE_CHAR);
        objPrepAction.setIs_show(true);

        objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(1);

        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Character to be removed");
        objCustomValue.setUi(Arrays.asList(objCustomTextValue));

        objPrepAction.setCustom_value(objCustomValue);

        lstDataReplaceGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_REPLACE_REMOVE_NULLITY);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_REPLACE_REMOVE_NULLITY);
        objPrepAction.setIs_show(true);

        objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(1);

        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Replace NULLITY value by");
        objCustomValue.setUi(Arrays.asList(objCustomTextValue));

        objPrepAction.setCustom_value(objCustomValue);

        lstDataReplaceGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_REPLACE_REMOVE_MISMATCH);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_REPLACE_REMOVE_MISMATCH);
        objPrepAction.setIs_show(true);

        objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(1);

        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Replace MISMATCH value by");
        objCustomValue.setUi(Arrays.asList(objCustomTextValue));

        objPrepAction.setCustom_value(objCustomValue);

        lstDataReplaceGeneralAction.add(objPrepAction);

        objPrepAction = new ESPrepActionModel();
        objPrepAction.setAction_id(ESFilterOperationConstant.DATA_REPLACE_REPLACE_TEXT);
        objPrepAction.setAction_name(ESFilterOperationConstant.DATA_REPLACE_REPLACE_TEXT);
        objPrepAction.setIs_show(true);

        List<ESFilterTextDefaultModel> lstCustomValue = new ArrayList<>();

        objCustomValue = new ESFilterCustomModel();
        objCustomValue.setIs_single_value(true);
        objCustomValue.setNum_of_input(2);

        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(1);
        objCustomTextValue.setPlace_holder("Find");
        lstCustomValue.add(objCustomTextValue);

        objCustomTextValue = new ESFilterTextDefaultModel();
        objCustomTextValue.setInput_idx(2);
        objCustomTextValue.setPlace_holder("Replace");
        lstCustomValue.add(objCustomTextValue);

        objCustomValue.setUi(lstCustomValue);

        objPrepAction.setCustom_value(objCustomValue);
        lstDataReplaceGeneralAction.add(objPrepAction);

        ESPrepActionTypeModel objDataReplaceActionType = new ESPrepActionTypeModel();
        objDataReplaceActionType.setId(ESFilterOperationConstant.PREP_OP_TYPE_DATA_REPLACE);
        objDataReplaceActionType.setValue("Replace");
        objDataReplaceActionType.setActions(lstDataReplaceGeneralAction);

        lstNumberActionTypeModel.add(objNumberDataTypeActionType);

        lstTextActionTypeModel.add(objTextDataTypeActionType);
        lstTextActionTypeModel.add(objDataFormatActionType);
        lstTextActionTypeModel.add(objSeparateActionTypeModel);
        lstTextActionTypeModel.add(objDataReplaceActionType);

        //SEPARATOR

        mapResult.put(ESFilterOperationConstant.DATA_TYPE_TEXT, lstTextActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_INTEGER, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_LONG, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_DOUBLE, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_BYTE, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_FLOAT, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_SHORT, lstNumberActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_DATE, lstTextActionTypeModel);
        mapResult.put(ESFilterOperationConstant.DATA_TYPE_BOOLEAN, lstTextActionTypeModel);

        return mapResult;
    }

    protected BulkProcessor createBulkProcessor(TransportClient objESClient, Integer intDataSize) {
        Integer intNumOfThread = Runtime.getRuntime().availableProcessors();

        intNumOfThread = intNumOfThread < 8 ? 8 : (intNumOfThread * 2);

        BulkProcessor objBulkProcessor = BulkProcessor.builder(objESClient, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    for (int i = 0; i < bulkResponse.getItems().length; i++) {
                        if (false == bulkResponse.getItems()[i].isFailed()) {
                            continue;
                        }
                        objLogger.warn("[" + bulkRequest.requests().get(i) + "]: [" + ExceptionUtil.getStackTrace(bulkResponse.getItems()[i].getFailure().getCause()) + "]");
                    }
                }

                objLogger.debug("INFO: After Bulk - " + String.valueOf(l) + " - "
                        + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                objLogger.debug("ERR: After Bulk with Throwable - " + String.valueOf(l) + " - "
                        + bulkRequest.numberOfActions());
                objLogger.warn("ERR: " + ExceptionUtil.getStackTrace(throwable));
            }
        }).setBulkActions(intDataSize < intNumBulkOperation ? intDataSize : intNumBulkOperation)
                .setConcurrentRequests(intNumOfThread)
                .build();

        return objBulkProcessor;
    }

    @SuppressWarnings("unchecked")
    public Boolean createIndexFromOtherIndex(String strIndex, String strType, String strFromIndex, String strFromType,
                                             List<String> lstRemoveField, HashMap<String, String> mapCopyField) {
        Boolean bIsCreated = false;

        try {
            if (objESClient != null) {
                GetFieldMappingsResponse objFieldMappingResponse = objESClient.admin().indices()
                        .prepareGetFieldMappings(strFromIndex).addTypes(strFromType).get();

                if (objFieldMappingResponse != null && objFieldMappingResponse.mappings() != null
                        && objFieldMappingResponse.mappings().size() > 0) {
                    HashMap<String, ESMappingFieldModel> mapFieldMapping = new HashMap<>();

                    // TODO curIndex always has size 0
                    for (Map.Entry<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> curIndex : objFieldMappingResponse
                            .mappings().entrySet()) {
                        for (Map.Entry<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> curType : curIndex
                                .getValue().entrySet()) {
                            for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetaData> curField : curType
                                    .getValue().entrySet()) {
                                if (!curField.getKey().contains(".keyword") && !curField.getKey().equals("_index")
                                        && !curField.getKey().equals("_all") && !curField.getKey().equals("_parent")
                                        && !curField.getKey().equals("_version")
                                        && !curField.getKey().equals("_routing") && !curField.getKey().equals("_type")
                                        && !curField.getKey().equals("_seq_no")
                                        && !curField.getKey().equals("_field_names")
                                        && !curField.getKey().equals("_source") && !curField.getKey().equals("_id")
                                        && !curField.getKey().equals("_uid")) {
                                    ESMappingFieldModel objFieldModel = new ESMappingFieldModel();
                                    objFieldModel.setIndex(null);
                                    objFieldModel.setFielddata(null);
                                    objFieldModel.setType(null);
                                    objFieldModel.setCopy_to(null);

                                    Map<String, Object> mapProperty = curField.getValue().sourceAsMap();

                                    if (mapProperty != null && mapProperty.size() > 0
                                            && mapProperty.get(curField.getValue().fullName()) instanceof HashMap) {
                                        HashMap<String, Object> mapCurType = ((HashMap<String, Object>) mapProperty
                                                .get(curField.getValue().fullName()));

                                        if (mapCurType != null && mapCurType.containsKey("type")) {
                                            String strFieldDataType = mapCurType.get("type").toString();

                                            if (strFieldDataType.equals("text")) {
                                                objFieldModel.setFielddata(true);
                                            }

                                            if (strFieldDataType.equals("keyword")) {
                                                objFieldModel.setIndex(true);
                                            }

                                            objFieldModel.setType(mapCurType.get("type").toString());
                                        }
                                    }

                                    mapFieldMapping.put(curField.getValue().fullName(), objFieldModel);
                                }
                            }

                            break;
                        }

                        break;
                    }

                    // Remove fields from mapCopyField that exists in lstRemoveField
                    // Remove fields from mapFieldMapping that exists in lstRemoveField
                    for (int intCount = 0; intCount < lstRemoveField.size(); intCount++) {
                        if (mapCopyField.containsKey(lstRemoveField.get(intCount))) {
                            mapCopyField.remove(lstRemoveField.get(intCount));
                        }

                        if (mapFieldMapping.containsKey(lstRemoveField.get(intCount))) {
                            mapFieldMapping.remove(lstRemoveField.get(intCount));
                        }
                    }

                    // Add copy_to property to field that exists in mapCopyField
                    // Add new field that related with above field in mapCopyField
                    for (Map.Entry<String, String> curCopyField : mapCopyField.entrySet()) {
                        if (mapFieldMapping.containsKey(curCopyField.getKey())) {
                            mapFieldMapping.get(curCopyField.getKey()).setCopy_to(curCopyField.getValue());

                            if (mapFieldMapping.containsKey(curCopyField.getValue())) {
                                ESMappingFieldModel objOldField = mapFieldMapping.get(curCopyField.getKey());
                                ESMappingFieldModel objNewField = new ESMappingFieldModel();
                                objNewField.setCopy_to(objOldField.getCopy_to());
                                objNewField.setType(objOldField.getType());
                                objNewField.setFielddata(objOldField.getFielddata());

                                mapFieldMapping.put(curCopyField.getValue(), objNewField);
                            }
                        }
                    }

                    if (mapFieldMapping != null && mapFieldMapping.size() > 0) {
                        bIsCreated = objESConnection.createIndex(strIndex, strType, null, null, mapFieldMapping, false);
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return bIsCreated;
    }

    protected List<String> generateFieldPrepActionScript(ESPrepAbstractModel objPrepAction) {
        List<String> lstScript = new ArrayList<>();

        if (objPrepAction instanceof ESPrepFieldModel) {
            ESPrepFieldModel objPrep = (ESPrepFieldModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                //Generate remove script field
                if (objPrep.getRemove_fields() != null && objPrep.getRemove_fields().size() > 0) {
                    for (String strField : objPrep.getRemove_fields()) {
                        String[] multipleFields = strField.split(",");
                        for (int i = 0; i < multipleFields.length; i++) {
                            String strRemoveScript = "ctx._source.remove(\"" + multipleFields[i].trim() + "\")";
                            lstScript.add(strRemoveScript);
                        }
                    }
                }

                //Generate copy script
                if (objPrep.getCopy_from_fields() != null && objPrep.getCopy_to_fields() != null
                        && objPrep.getCopy_to_fields().size() == objPrep.getCopy_from_fields().size()) {
                    for (int intCountCopy = 0; intCountCopy < objPrep.getCopy_from_fields()
                            .size(); intCountCopy++) {
                        String strFromField = ConverterUtil.convertDashField(objPrep.getCopy_to_fields().get(intCountCopy));
                        String strToField = ConverterUtil.convertDashField(objPrep.getCopy_from_fields().get(intCountCopy));

                        String strCopyScript = "ctx._source" + strFromField + " = ctx._source" + strToField;
                        lstScript.add(strCopyScript);
                    }
                }
            }
        }

        return lstScript;
    }

    protected List<String> generateDataTypeConvertScript(String strField, String strNewField, String strConvertedDataType,
                                                         String strDateFormat, String strFailedDefaultValue) {
        String strConvertScript = "";
        String strCatchConvertScript = "";

        strField = ConverterUtil.convertDashField(strField);
        strNewField = ConverterUtil.convertDashField(strNewField);

        try {
            String strOldField = "ctx._source" + strField;
            StringBuilder objBuilder = new StringBuilder();
            StringBuilder objCatchBuilder = new StringBuilder();

            objBuilder.append("ctx._source").append(strNewField).append(" = ");
            objCatchBuilder.append("ctx._source").append(strNewField).append(" = ");

            switch (strConvertedDataType) {
                case ESFilterOperationConstant.DATA_TYPE_BOOLEAN:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "false" : strFailedDefaultValue;
                    objBuilder.append("Boolean.parseBoolean(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_BYTE:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "0" : strFailedDefaultValue;
                    objBuilder.append("Byte.parseByte(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_DOUBLE:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "0.0" : strFailedDefaultValue;
                    objBuilder.append("Double.parseDouble(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_DATE:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "Calendar.getInstance().getTimeInMillis()"
                            : strFailedDefaultValue;
                    Calendar.getInstance().getTimeInMillis();
                    objBuilder.append("new SimpleDateFormat(").append(strDateFormat).append(").parse(").append(strOldField)
                            .append(").getTimeInMillis();");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_FLOAT:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "OF" : strFailedDefaultValue;
                    objBuilder.append("Float.parseFloat(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_INTEGER:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "O" : strFailedDefaultValue;
                    objBuilder.append("Integer.parseInt(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_LONG:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "OL" : strFailedDefaultValue;
                    objBuilder.append("Long.parseLong(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_NUMERIC:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "O.0" : strFailedDefaultValue;
                    objBuilder.append("Double.parseDouble(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_SHORT:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "O" : strFailedDefaultValue;
                    objBuilder.append("Short.parseShort(").append(strOldField).append(");");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
                case ESFilterOperationConstant.DATA_TYPE_TEXT:
                    strFailedDefaultValue = strFailedDefaultValue.isEmpty() ? "\"\"" : ("\"" + strFailedDefaultValue + "\"");
                    objBuilder.append(strOldField).append(".toString();");
                    objCatchBuilder.append(strFailedDefaultValue).append(";");
                    break;
            }

            strConvertScript = objBuilder.toString();
            strCatchConvertScript = objCatchBuilder.toString();
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return new ArrayList<>(Arrays.asList(strConvertScript, strCatchConvertScript));
    }

    protected String generateChangeFieldDataTypeScript(String strField,
                                                       String newFieldName, String strConvertedDataType,
                                                       Boolean bIsForce, String strFailedDefaultValue, String strDateFormat) {
        String strPainlessScript = "";
        String strConvertScript = "";
        String strConvertCatchScript = "";

        List<String> lstConvertScript = generateDataTypeConvertScript(strField, newFieldName,
                strConvertedDataType, strDateFormat, strFailedDefaultValue);
        strConvertScript = lstConvertScript.get(0);
        strConvertCatchScript = lstConvertScript.get(1);

        if (!bIsForce) {
            strPainlessScript = new StringBuilder().append("try { ").append(strConvertScript).append(
                    " } catch (Exception objEx) { throw new RuntimeException(\"Can't not convert data\"); }")
                    .toString();
        } else {
            strPainlessScript = new StringBuilder().append("try { ").append(strConvertScript)
                    .append(" } catch (Exception objEx) { ").append(strConvertCatchScript).append(" }")
                    .toString();
        }

        return strPainlessScript;
    }

    protected String generateFormatDataScript(String strField, String newFieldName,
                                              String strFormatOperation, String strFormatParam1, String strFormatParam2) {
        String strFormatScript = "";

        strField = ConverterUtil.convertDashField(strField);
        newFieldName = ConverterUtil.convertDashField(newFieldName);

        switch (strFormatOperation) {
            case ESFilterOperationConstant.DATA_FORMAT_LOWERCASE:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(".toLowerCase();").toString();
                break;
            case ESFilterOperationConstant.DATA_FORMAT_UPPERCASE:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(".toUpperCase();").toString();
                break;
            case ESFilterOperationConstant.DATA_FORMAT_ADD_POSTFIX:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(" + \"").append(strFormatParam1)
                        .append("\";").toString();
                break;
            case ESFilterOperationConstant.DATA_FORMAT_ADD_PREFIX:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName).append(" = \"")
                        .append(strFormatParam1).append("\" + ").append(strField).append(";").toString();
                break;
            case ESFilterOperationConstant.DATA_REPLACE_REMOVE_CHAR:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(".replace(\"").append(strFormatParam1)
                        .append("\", \"\");").toString();
                break;
            case ESFilterOperationConstant.DATA_REPLACE_REMOVE_WHITE_SPACE:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(".replaceAll(\"\\s+\", \"\");")
                        .toString();
                break;
            case ESFilterOperationConstant.DATA_REPLACE_REPLACE_POS:
                break;
            case ESFilterOperationConstant.DATA_REPLACE_REPLACE_TEXT:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strField).append(".replace(\"").append(strFormatParam1)
                        .append("\", \"").append(strFormatParam2).append("\");").toString();
                break;
        }

        return strFormatScript;
    }

    protected Boolean handleFields(String strIndex, String strType, ESPrepFieldModel objPrepFieldModel) {
        Boolean bIsFinish = true;

        //0. Create new field
        if (objPrepFieldModel.getCopy_from_fields() != null && objPrepFieldModel.getCopy_to_fields() != null
                && objPrepFieldModel.getCopy_from_fields().size() == objPrepFieldModel.getCopy_to_fields().size()) {
            for (int intCount = 0; intCount < objPrepFieldModel.getCopy_from_fields().size(); intCount++) {
                List<String> lstNewFieldInfo = getNewFieldFromAction(objPrepFieldModel,
                        objPrepFieldModel.getCopy_from_fields().get(intCount),
                        objPrepFieldModel.getCopy_to_fields().get(intCount));

                if (lstNewFieldInfo != null && lstNewFieldInfo.size() == 2) {
                    Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties
                            = objESConnection.createNewMappingField(lstNewFieldInfo.get(1), lstNewFieldInfo.get(0));
                    AcknowledgedResponse objPutMappingResponse = null;

                    try {
                        objPutMappingResponse = objESClient.admin().indices().preparePutMapping(strIndex)
                                .setType(strType)
                                .setSource(objMapper.writeValueAsString(mapFieldProperties), XContentType.JSON).get();
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (objPutMappingResponse != null && objPutMappingResponse.isAcknowledged()) {
                        try {
                            bIsFinish = true;
                        } catch (Exception objEx) {
                            bIsFinish = false;
                        }
                    } else {
                        bIsFinish = false;
                    }
                }
            }
        }

        if (bIsFinish) {
            //1. Scroll all rows
            SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intNumBulkOperation).get();

            Long lCurNumHit = 0l;
            Long lTotalHit = 0l;
            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    lTotalHit = objSearchResponse.getHits().getTotalHits();
                    lCurNumHit += objSearchResponse.getHits().getHits().length;

                    try {
                        //2. With each scroll time, bulk update
                        BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intNumBulkOperation);

                        //2.1. Create Elastic Script that is related with current action
                        List<String> lstScript = generateFieldPrepActionScript(objPrepFieldModel);

                        if (lstScript != null && lstScript.size() > 0) {
                            //2.2. Create Update Request and add to bulk processor
                            for (int intCountScript = 0; intCountScript < lstScript.size(); intCountScript++) {
                                String strScript = lstScript.get(intCountScript);

                                for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                                    UpdateRequest objUpdateRequest = new UpdateRequest(strIndex, strType, objHit.getId());
                                    objUpdateRequest.script(new Script(strScript));

                                    objBulkProcessor.add(objUpdateRequest);
                                }
                            }
                        }

                        objBulkProcessor.flush();
                        objBulkProcessor.awaitClose(10l, TimeUnit.MINUTES);
                    } catch (Exception objEx) {
                        bIsFinish = false;
                        objLogger.debug(ExceptionUtil.getStackTrace(objEx));

                        break;
                    }

                    objLogger.debug("Cur Hit: " + lCurNumHit);
                    objLogger.debug("Total Hits: " + lTotalHit);

                    //3. Continue to scroll
                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lScrollTTL)).get();
                } else {
                    break;
                }
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);
        }

        //3. Return
        return bIsFinish;
    }

    protected Boolean handleDocuments(String strIndex, String strType, List<String> lstRemoveRowIdx,
                                      HashMap<String, Integer> mapCopyRowIdx) {
        Boolean bIsHandled = false;

        try {
            if (objESClient != null) {
                Boolean bIsExistIndex = false;
                Boolean bIsExistType = false;

                // Check Index and Type is existed
                List<ESIndexModel> lstIndices = objESConnection.getAllIndices();

                for (int intCount = 0; intCount < lstIndices.size(); intCount++) {
                    if (lstIndices.get(intCount).getIndex_name().equals(strIndex)) {
                        bIsExistIndex = true;

                        for (int intCountType = 0; intCountType < lstIndices.get(intCount).getIndex_types()
                                .size(); intCountType++) {
                            if (lstIndices.get(intCount).getIndex_types().get(intCountType).equals(strType)) {
                                bIsExistType = true;
                                break;
                            }
                        }

                        break;
                    }
                }

                if (bIsExistIndex && bIsExistType) {
                    // Check if remove-doc indx is existed in mapCopyRowIdx
                    for (int intCount = 0; intCount < lstRemoveRowIdx.size(); intCount++) {
                        if (mapCopyRowIdx.containsKey(lstRemoveRowIdx.get(intCount))) {
                            mapCopyRowIdx.remove(lstRemoveRowIdx.get(intCount));
                        }
                    }

                    // Remove all documents by lstRemoveRowIdx
                    for (int intCount = 0; intCount < lstRemoveRowIdx.size(); intCount++) {
                        DeleteResponse objDeleteResponse = objESClient
                                .prepareDelete(strIndex, strType, lstRemoveRowIdx.get(intCount)).get();
                        if (objDeleteResponse != null && objDeleteResponse.getResult() != null
                                && objDeleteResponse.getResult().getLowercase().equals("deleted")) {
                            bIsHandled = true;
                        } else {
                            bIsHandled = false;
                            break;
                        }
                    }

                    // Copy document to new documents
                    for (Map.Entry<String, Integer> curCopiedDoc : mapCopyRowIdx.entrySet()) {
                        String strCopiedDocIdx = curCopiedDoc.getKey();
                        String strSourceDoc = "";
                        GetResponse objGetResponse = objESClient.prepareGet(strIndex, strType, strCopiedDocIdx).get();

                        if (objGetResponse != null && objGetResponse.isExists() && !objGetResponse.isSourceEmpty()) {
                            strSourceDoc = objGetResponse.getSourceAsString();
                        }

                        if (strSourceDoc != null && !strSourceDoc.isEmpty()) {
                            for (int intCount = 1; intCount <= curCopiedDoc.getValue(); intCount++) {
                                String strNewDocIdx = strCopiedDocIdx + "_" + intCount;
                                IndexResponse objIndexResponse = objESClient
                                        .prepareIndex(strIndex, strType, strNewDocIdx)
                                        .setSource(strSourceDoc, XContentType.JSON).get();

                                if (objIndexResponse != null && objIndexResponse.getResult() != null
                                        && objIndexResponse.getResult().getLowercase().equals("created")) {
                                    bIsHandled = true;
                                } else {
                                    bIsHandled = false;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return bIsHandled;
    }

    protected String generatePrepActionScript(ESPrepAbstractModel objPrepAction) {
        String strScript = "";
        String strCurIndex = objPrepAction.getIndex();

        if (objPrepAction instanceof ESPrepFormatModel) {
            ESPrepFormatModel objPrep = (ESPrepFormatModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                strScript = generateFormatDataScript(objPrep.getField(), objPrep.getNew_field_name(),
                        objPrep.getFormat_op(), objPrep.getFormat_param_1(),
                        objPrep.getFormat_param_2());
            }
        }

        if (objPrepAction instanceof ESPrepDataTypeChangeModel) {
            ESPrepDataTypeChangeModel objPrep = (ESPrepDataTypeChangeModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                strScript = generateChangeFieldDataTypeScript(objPrep.getField(), objPrep.getNew_field_name(),
                        objPrep.getConverted_data_type(), objPrep.getIs_forced(),
                        objPrep.getFailed_default_value(), objPrep.getDate_format());
            }
        }

        if (objPrepAction instanceof ESPrepFunctionArithmeticModel) {
            ESPrepFunctionArithmeticModel objPrep = (ESPrepFunctionArithmeticModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                strScript = generateArithmeticFunctionScript(objPrep.getField(), objPrep.getNew_field_name(),
                        objPrep.getArithmetic_op(), objPrep.getArithmetic_param_1(), objPrep.getArithmetic_param_2());
            }
        }

        if (objPrepAction instanceof ESPrepFunctionStatisticModel) {
            ESPrepFunctionStatisticModel objPrep = (ESPrepFunctionStatisticModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                ESFieldStatModel objStatField = null;

                if (objPrep.getStatistic_op().equals(ESFilterOperationConstant.FUNCTION_STATISTICS_STANDARD)
                        || objPrep.getStatistic_op().equals(ESFilterOperationConstant.FUNCTION_STATISTICS_NORM)) {
                    Map<String, ESFieldStatModel> mapStat = objESFilter.statsField(objPrep.getIndex(), objPrep.getType(), Arrays.asList(objPrep.getSelected_field().get(0)), null, true);

                    if (mapStat != null && mapStat.containsKey(objPrep.getSelected_field().get(0))) {
                        objStatField = mapStat.get(objPrep.getSelected_field().get(0));
                    }
                }

                strScript = generateStatisticFunctionScript(objPrep, objStatField);
            }
        }

        return strScript;
    }

    protected HashMap<String, ArrayList<Object>> handleSimpleSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        HashMap<String, ArrayList<Object>> mapGenerateValue = new HashMap<>();

        Long lNumOfRow = objPrep.getNum_of_rows();
        List<Double> lstDefinedValue = objPrep.getDefined_values();
        List<String> lstSelectedField = objPrep.getSelected_fields();

        if (lstDefinedValue != null && lstDefinedValue.size() > 0) {
            if (lNumOfRow == -1) { //Number of rows = Number of documents
                lNumOfRow = objESFilter.getTotalHit(objPrep.getIndex(), objPrep.getType());
            }

            if ((lstSelectedField == null || lstSelectedField.size() == 0) && lstDefinedValue.size() >= 3) {
                //Random Sampling by Min and Max
                Boolean bCanDuplicate = lstDefinedValue.get(0).equals(1.0) ? true : false;
                Double dbMin = lstDefinedValue.get(1);
                Double dbMax = lstDefinedValue.get(2);

                ArrayList<Object> lstRandValue = new ArrayList<>();

                for (long lCount = 0L; lCount < lNumOfRow; lCount++) {
                    Double dbRand = ConverterUtil.randomDouble(objRandom, dbMin, dbMax);

                    if (bCanDuplicate) {
                        lstRandValue.add(dbRand);
                    } else {
                        while (lstRandValue.contains(dbRand)) {
                            dbRand = ConverterUtil.randomDouble(objRandom, dbMin, dbMax);

                            if (!lstRandValue.contains(dbRand)) {
                                lstRandValue.add(dbRand);
                                break;
                            }
                        }
                    }
                }

                mapGenerateValue.put(objPrep.getNew_fields().get(0), lstRandValue);
            } else if (lstSelectedField.size() == 1) {
                ArrayList<Object> lstRandValue = new ArrayList<>();

                //Random selected values from selected column
                //Using scroll api to select random data
                SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(objPrep.getIndex()).setTypes(objPrep.getType())
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                        .setSize(intNumBulkOperation);

                SearchSourceBuilder objSourceBuilder = new SearchSourceBuilder();
                objSourceBuilder.query(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), ScoreFunctionBuilders.randomFunction()));
                objSourceBuilder.fetchSource(lstSelectedField.get(0), null);
                objRequestBuilder.setSource(objSourceBuilder);

                SearchResponse objSearchResponse = objRequestBuilder.get();

                Long lCurNumHit = 0l;

                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {

                        for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                            lCurNumHit += 1;
                            Double dbValue = (Double) objHit.getSourceAsMap().get(lstSelectedField.get(0));
                            lstRandValue.add(dbValue);

                            if (lCurNumHit >= lNumOfRow) {
                                break;
                            }
                        }
                    }

                    if (lCurNumHit >= lNumOfRow) {
                        break;
                    }
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);

                mapGenerateValue.put(objPrep.getNew_fields().get(0), lstRandValue);
            } else if (lstSelectedField.size() > 1) {
                //Random selected values' indices from 1st selected column and apply for other columns
                //Random selected values from selected column
                //Using scroll api to select random data
                SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(objPrep.getIndex()).setTypes(objPrep.getType())
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                        .setSize(intNumBulkOperation);

                SearchSourceBuilder objSourceBuilder = new SearchSourceBuilder();
                objSourceBuilder.query(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), ScoreFunctionBuilders.randomFunction()));
                objSourceBuilder.fetchSource(lstSelectedField.toArray(new String[lstSelectedField.size()]), null);
                objRequestBuilder.setSource(objSourceBuilder);

                SearchResponse objSearchResponse = objRequestBuilder.get();

                Long lCurNumHit = 0l;

                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {

                        for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                            lCurNumHit += 1;

                            for (int intCount = 0; intCount < lstSelectedField.size(); intCount++) {
                                String strNewField = objPrep.getNew_fields().get(intCount);
                                Double dbValue = (Double) objHit.getSourceAsMap().get(lstSelectedField.get(intCount));

                                if (mapGenerateValue.containsKey(strNewField)) {
                                    mapGenerateValue.get(strNewField).add(dbValue);
                                } else {
                                    mapGenerateValue.put(strNewField, new ArrayList<>(Arrays.asList(dbValue)));
                                }
                            }

                            if (lCurNumHit >= lNumOfRow) {
                                break;
                            }
                        }
                    }

                    if (lCurNumHit >= lNumOfRow) {
                        break;
                    }
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);
            }
        }

        return mapGenerateValue;
    }

    protected HashMap<String, ArrayList<Object>> handleSystematicSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        HashMap<String, ArrayList<Object>> mapGenerateValue = new HashMap<>();

        Long lNumOfRow = objESFilter.getTotalHit(objPrep.getIndex(), objPrep.getType());
        List<Double> lstDefinedValue = objPrep.getDefined_values();
        List<String> lstSelectedField = objPrep.getSelected_fields();

        if (lstDefinedValue != null && lstDefinedValue.size() >= 1 && lstSelectedField != null && lstSelectedField.size() == 1) {
            //Random Sampling by Min and Max
            Double dbStep = lstDefinedValue.get(0);
            dbStep = dbStep <= 0 ? 1 : dbStep;

            ArrayList<Object> lstRandValue = new ArrayList<>();
            //Random selected values from selected column
            //Using scroll api to select random data
            SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(objPrep.getIndex()).setTypes(objPrep.getType())
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intNumBulkOperation);

            SearchSourceBuilder objSourceBuilder = new SearchSourceBuilder();
            objSourceBuilder.query(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), ScoreFunctionBuilders.randomFunction()));
            objSourceBuilder.fetchSource(lstSelectedField.get(0), null);
            objRequestBuilder.setSource(objSourceBuilder);

            SearchResponse objSearchResponse = objRequestBuilder.get();
            Long lTotalHit = objESFilter.getTotalHit(objPrep.getIndex(), objPrep.getType());
            Long lCurNumHit = 0L;

            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    lCurNumHit += objSearchResponse.getHits().getHits().length;

                    for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                        Double dbValue = (Double) objHit.getSourceAsMap().get(lstSelectedField.get(0));
                        lstRandValue.add(dbValue);
                    }

                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lScrollTTL)).get();
                } else {
                    break;
                }
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);

            ArrayList<Object> lstFinal = new ArrayList<>();

            for (long lCount = 0L; lCount < lstRandValue.size(); lCount += dbStep.longValue()) {
                lstFinal.add(lstRandValue.get((int) lCount));
            }

            mapGenerateValue.put(objPrep.getNew_fields().get(0), lstFinal);
        }

        return mapGenerateValue;
    }

    protected HashMap<String, ArrayList<Object>> handleDistributionSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        HashMap<String, ArrayList<Object>> mapGenerateValue = new HashMap<>();

        Long lNumOfRow = objESFilter.getTotalHit(objPrep.getIndex(), objPrep.getType());
        List<Double> lstDefinedValue = objPrep.getDefined_values();
        List<String> lstSelectedField = objPrep.getSelected_fields();

        if (objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_DIST_GAUSSIAN)) {
            if (lstDefinedValue != null && lstDefinedValue.size() >= 2) {
                Double dbMean = lstDefinedValue.get(0);
                Double dbSd = lstDefinedValue.get(1);
                ArrayList<Object> lstFinal = new ArrayList<>(MathUtil.getGaussianDistribution(dbMean, dbSd, lNumOfRow));

                mapGenerateValue.put(objPrep.getNew_fields().get(0), lstFinal);
            }
        } else if (objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_DIST_CONTINUOUS)) {
            if (lstDefinedValue != null && lstDefinedValue.size() >= 2) {
                Double dbMin = lstDefinedValue.get(0);
                Double dbMax = lstDefinedValue.get(1);
                ArrayList<Object> lstFinal = new ArrayList<>(MathUtil.getContinuousUniformDistribution(dbMin, dbMax, lNumOfRow));

                mapGenerateValue.put(objPrep.getNew_fields().get(0), lstFinal);
            }
        }

        return mapGenerateValue;
    }

    protected Boolean handleSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        //0. Create new field
        Boolean bIsFinish = false;

        if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
            for (int intCount = 0; intCount < objPrep.getNew_fields().size(); intCount++) {
                List<String> lstNewFieldInfo = getNewFieldFromAction(objPrep,
                        objPrep.getNew_fields().get(intCount),
                        objPrep.getNew_fields().get(intCount));

                if (lstNewFieldInfo != null && lstNewFieldInfo.size() == 2) {
                    Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties
                            = objESConnection.createNewMappingField(lstNewFieldInfo.get(1), lstNewFieldInfo.get(0));
                    AcknowledgedResponse objPutMappingResponse = null;

                    try {
                        objPutMappingResponse = objESClient.admin().indices().preparePutMapping(objPrep.getIndex())
                                .setType(objPrep.getType())
                                .setSource(objMapper.writeValueAsString(mapFieldProperties), XContentType.JSON).get();
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    if (objPutMappingResponse != null && objPutMappingResponse.isAcknowledged()) {
                        try {
                            bIsFinish = true;
                        } catch (Exception objEx) {
                            bIsFinish = false;
                        }
                    } else {
                        bIsFinish = false;
                    }
                }
            }
        }

        if (bIsFinish) {
            //1. Scroll all rows

            SearchResponse objSearchResponse = objESClient.prepareSearch(objPrep.getIndex()).setTypes(objPrep.getType())
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intNumBulkOperation).get();

            Long lCurNumHit = 0L;
            Long lTotalHit = 0L;
            Long lCurUpdateHit = 0L;

            HashMap<String, ArrayList<Object>> mapSamplingValue = new HashMap<>();

            if (objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_SIMPLE)) {
                mapSamplingValue = handleSimpleSamplingAction(objPrep);
            } else if (objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_SYSTEMATIC)) {
                mapSamplingValue = handleSystematicSamplingAction(objPrep);
            } else if (objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_DIST_GAUSSIAN)
                    || objPrep.getSampling_op().equals(ESFilterOperationConstant.FUNCTION_SAMPLING_DIST_CONTINUOUS)) {
                mapSamplingValue = handleDistributionSamplingAction(objPrep);
            }

            if (mapSamplingValue != null && mapSamplingValue.size() > 0) {
                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {
                        lTotalHit = objSearchResponse.getHits().getTotalHits();
                        lCurNumHit += objSearchResponse.getHits().getHits().length;

                        try {
                            //2. With each scroll time, bulk update
                            BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intNumBulkOperation);

                            for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                                for (Map.Entry<String, ArrayList<Object>> item : mapSamplingValue.entrySet()) {
                                    if (item.getValue() != null && item.getValue().size() > lCurUpdateHit) {
                                        String strScript = "ctx._source" + ConverterUtil.convertDashField(item.getKey()) + " = " + item.getValue().get(lCurUpdateHit.intValue()).toString();

                                        UpdateRequest objUpdateRequest = new UpdateRequest(objPrep.getIndex(), objPrep.getType(), objHit.getId());
                                        objUpdateRequest.script(new Script(strScript));

                                        objBulkProcessor.add(objUpdateRequest);
                                    }
                                }

                                lCurUpdateHit += 1;
                            }

                            objBulkProcessor.flush();
                            objBulkProcessor.awaitClose(10l, TimeUnit.MINUTES);
                        } catch (Exception objEx) {
                            bIsFinish = false;
                            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));

                            break;
                        }

                        objLogger.debug("Cur Hit: " + lCurNumHit);
                        objLogger.debug("Total Hits: " + lTotalHit);

                        //3. Continue to scroll
                        objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                .setScroll(new TimeValue(lScrollTTL)).get();
                    } else {
                        break;
                    }
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);
            }
        }

        //3. Return
        return bIsFinish;
    }

    protected String generateStatisticFunctionScript(ESPrepFunctionStatisticModel objPrep, ESFieldStatModel objFieldStat) {
        StringBuilder strScript = new StringBuilder();
        String strNewFieldName = ConverterUtil.convertDashField(objPrep.getNew_field_name());

        List<Double> lstTest = Arrays.asList(1.0, 2.0, 3.0);
        StringBuilder strArrField = new StringBuilder();
        strArrField.append("Arrays.asList(new Double[] {");
        strArrField.append(objPrep.getSelected_field().stream().map(str -> "ctx._source" + ConverterUtil.convertDashField(str)).collect(Collectors.joining(",")));
        strArrField.append("}).stream().mapToDouble(num -> num)");

        StringBuilder strArrNotMapField = new StringBuilder();
        strArrNotMapField.append("Arrays.asList(new Double[] {");
        strArrNotMapField.append(objPrep.getSelected_field().stream().map(str -> "ctx._source" + ConverterUtil.convertDashField(str)).collect(Collectors.joining(",")));
        strArrNotMapField.append("}).stream()");

        String strNewField = "ctx._source" + strNewFieldName;

        switch (objPrep.getStatistic_op()) {
            case ESFilterOperationConstant.FUNCTION_STATISTICS_SUM:
                //lstTest.stream().mapToDouble(a -> a).sum();
                strScript.append(strNewField).append(" = ").append(strArrField.toString());
                strScript.append(".sum()");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_MIN:
                //lstTest.stream().mapToDouble(a -> a).min().getAsDouble();
                strScript.append(strNewField).append(" = ").append(strArrField.toString());
                strScript.append(".min().getAsDouble()");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_MAX:
                strScript.append(strNewField).append(" = ").append(strArrField.toString());
                strScript.append(".max().getAsDouble()");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_MEDIAN:
                //lstTest.stream().mapToDouble(a -> a).sorted().skip((3-1)/2).limit(2-3%2).average().orElse(Double.NaN);
                strScript.append(strNewField).append(" = ").append(strArrField.toString());
                strScript.append(".sorted().skip((").append(objPrep.getSelected_field().size()).append("-1)/2).limit(2-").append(objPrep.getSelected_field().size()).append("%2).average().orElse(Double.NaN)");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_MEAN:
                strScript.append(strNewField).append(" = ").append(strArrField.toString());
                strScript.append(".average().getAsDouble()");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_RANGE:
                strScript.append(strNewField).append(" = (").append(strArrField.toString());
                strScript.append(".max().getAsDouble() - ").append(strArrField.toString()).append(".min().getAsDouble())");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_VARIANCE:
                //double dbMean = lstTest.stream().mapToDouble(a -> a).average().getAsDouble();
                //double dbSum = lstTest.stream().mapToDouble(a -> Math.pow(a - dbMean, 2)).sum() / (3 - 1);
                strScript.append("double dbMean = ").append(strArrField.toString()).append(".average().getAsDouble(); ");
                strScript.append(strNewField).append(" = ").append(strArrNotMapField.toString()).append(".mapToDouble(num -> Math.pow(num - dbMean, 2)).sum() / ");
                strScript.append(objPrep.getSelected_field().size() - 1).append(";");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_STD:
                //double dbMean = lstTest.stream().mapToDouble(a -> a).average().getAsDouble();
                //double dbVar = lstTest.stream().mapToDouble(a -> Math.pow(a - dbMean, 2)).sum() / (3 - 1);
                //double dbStd = Math.sqrt(dbVar);
                strScript.append("double dbMean = ").append(strArrField.toString()).append(".average().getAsDouble(); ");
                strScript.append("double dbVar = ").append(strArrNotMapField.toString()).append(".mapToDouble(num -> Math.pow(num - dbMean, 2)).sum() / ");
                strScript.append(objPrep.getSelected_field().size() - 1).append("; ");
                strScript.append(strNewField).append(" = Math.sqrt(dbVar);");
                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_STANDARD:
                if (objFieldStat != null) {
                    Double dbMean = objFieldStat.getAvg();
                    Double dbStd = objFieldStat.getStd_deviation();
                    String strField = ConverterUtil.convertDashField(objPrep.getSelected_field().get(0));

                    strScript.append(strNewField).append(" = (").append("ctx._source").append(strField).append(" - ").append(dbMean.toString()).append(") / ").append(dbStd.toString()).append(";");
                }

                break;
            case ESFilterOperationConstant.FUNCTION_STATISTICS_NORM:
                if (objFieldStat != null) {
                    Double dbMin = objFieldStat.getMin();
                    Double dbMax = objFieldStat.getMax();
                    String strOldField = ConverterUtil.convertDashField(objPrep.getSelected_field().get(0));

                    strScript.append(strNewField).append(" = (").append("ctx._source").append(strOldField).append(" - ").append(dbMin.toString()).append(") / (")
                            .append(dbMax.toString()).append(" - ").append(dbMin.toString()).append(");");
                }

                break;
            default:
                break;
        }

        return strScript.toString();
    }

    protected String generateArithmeticFunctionScript(String strField, String newFieldName,
                                                      String strArithmeticOperation, String strArithmeticParam1, String strArithmeticParam2) {
        String strFormatScript = "";

        if (strField != null && !strField.isEmpty()) {
            strField = ConverterUtil.convertDashField(strField);
        }

        if (newFieldName != null && !newFieldName.isEmpty()) {
            newFieldName = ConverterUtil.convertDashField(newFieldName);
        }

        // TODO for unary operation, the calculation is based on the strField, and newFieldName
        switch (strArithmeticOperation) {
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_ADD:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strArithmeticParam1)
                        .append(" + ctx._source").append(strArithmeticParam2).toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_SUB:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strArithmeticParam1)
                        .append(" - ctx._source").append(strArithmeticParam2).toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_MULTIPLY:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strArithmeticParam1)
                        .append(" * ctx._source").append(strArithmeticParam2).toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_DIVIDE:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ctx._source").append(strArithmeticParam1)
                        .append(" / ctx._source").append(strArithmeticParam2).toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_SIN:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = Math.sin(ctx._source").append(strField).append(")").toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_COS:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = Math.cos(ctx._source").append(strField).append(")").toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_TAN:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = Math.tan(ctx._source").append(strField).append(")").toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = Math.log(ctx._source").append(strField).append(")").toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_LOG10:
                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = Math.log10(ctx._source").append(strField).append(")").toString();
                break;
            case ESFilterOperationConstant.FUNCTION_ARITHMETIC_FORMULA: //Ex: #sin($val$) * ($wind$ + $temp$)
                String strFormula = strArithmeticParam1;
                strFormula = strFormula.replace("#", "Math.");
                HashMap<String, String> mapField = new HashMap<>();

                Pattern objFieldPattern = Pattern.compile("(\\$)([^\\$]+)(\\$)");
                Matcher objMatcher = objFieldPattern.matcher(strFormula);

                while (objMatcher.find()) {
                    String strFoundField = objMatcher.group();
                    mapField.put(strFoundField, "ctx._source" + ConverterUtil.convertDashField(strFoundField.replace("$", "")));
                }

                for (Map.Entry<String, String> itemField : mapField.entrySet()) {
                    strFormula = strFormula.replace(itemField.getKey(), itemField.getValue());
                }

                strFormatScript = new StringBuilder().append("ctx._source").append(newFieldName)
                        .append(" = ").append(strFormula).toString();

                break;
        }

        return strFormatScript;
    }

    protected List<String> getNewFieldFromAction(ESPrepAbstractModel objPrepAction, String strOldField, String strNewField) {
        String strNewFieldName = "";
        String strNewFieldType = "";

        if (objPrepAction instanceof ESPrepFieldModel) {
            strNewFieldName = strNewField;
            List<ESFieldModel> lstField = objESConnection.getFieldsMetaData(objPrepAction.getIndex(), objPrepAction.getType(),
                    new ArrayList<>(Arrays.asList(strOldField)), true);

            strNewFieldType = lstField.get(0).getType();
        }

        if (objPrepAction instanceof ESPrepFormatModel) {
            ESPrepFormatModel objFormat = (ESPrepFormatModel) objPrepAction;
            strNewFieldName = objFormat.getNew_field_name();

            List<ESFieldModel> lstField = objESConnection.getFieldsMetaData(objFormat.getIndex(), objFormat.getType(),
                    new ArrayList<>(Arrays.asList(objFormat.getField())), true);

            strNewFieldType = lstField.get(0).getType();
        }

        if (objPrepAction instanceof ESPrepDataTypeChangeModel) {
            ESPrepDataTypeChangeModel objPrep = (ESPrepDataTypeChangeModel) objPrepAction;
            strNewFieldName = objPrep.getNew_field_name();
            strNewFieldType = objPrep.getConverted_data_type();
        }

        if (objPrepAction instanceof ESPrepFunctionArithmeticModel) {
            ESPrepFunctionArithmeticModel objPrep = (ESPrepFunctionArithmeticModel) objPrepAction;

            strNewFieldName = objPrep.getNew_field_name();
            strNewFieldType = "double";
        }

        if (objPrepAction instanceof ESPrepFunctionStatisticModel) {
            ESPrepFunctionStatisticModel objPrep = (ESPrepFunctionStatisticModel) objPrepAction;

            strNewFieldName = objPrep.getNew_field_name();
            strNewFieldType = "double";
        }

        if (objPrepAction instanceof ESPrepFunctionSamplingModel) {
            strNewFieldName = strNewField;
            strNewFieldType = "double";
        }

        List<String> lstNewField = new ArrayList<>();

        if (strNewFieldName != null && !strNewFieldName.isEmpty() && strNewFieldType != null && !strNewFieldType.isEmpty()) {
            lstNewField.add(strNewFieldName);
            lstNewField.add(strNewFieldType);
        }

        return lstNewField;
    }

    protected Boolean prepBulkAction(String strIndex, String strType, ESPrepAbstractModel objPrepAction, Integer intPageSize) {
        Boolean bIsFinish = true;

        //0. Create new field
        List<String> lstNewFieldInfo = getNewFieldFromAction(objPrepAction, "", "");

        if (lstNewFieldInfo != null && lstNewFieldInfo.size() == 2) {
            Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties
                    = objESConnection.createNewMappingField(lstNewFieldInfo.get(1), lstNewFieldInfo.get(0));
            AcknowledgedResponse objPutMappingResponse = null;

            try {
                objPutMappingResponse = objESClient.admin().indices().preparePutMapping(strIndex)
                        .setType(strType)
                        .setSource(objMapper.writeValueAsString(mapFieldProperties), XContentType.JSON).get();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            if (objPutMappingResponse != null && objPutMappingResponse.isAcknowledged()) {
                try {
                    bIsFinish = true;
                } catch (Exception objEx) {
                    bIsFinish = false;
                }
            } else {
                bIsFinish = false;
            }
        }

        if (bIsFinish) {
            //1. Scroll all rows
            SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intPageSize).get();

            Long lCurNumHit = 0l;
            Long lTotalHit = 0l;
            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    lTotalHit = objSearchResponse.getHits().getTotalHits();
                    lCurNumHit += objSearchResponse.getHits().getHits().length;

                    try {
                        //2. With each scroll time, bulk update
                        BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intNumBulkOperation);

                        //2.1. Create Elastic Script that is related with current action
                        String strScript = generatePrepActionScript(objPrepAction);

                        if (strScript != null && !strScript.isEmpty()) {
                            //2.2. Create Update Request and add to bulk processor
                            for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                                UpdateRequest objUpdateRequest = new UpdateRequest(strIndex, strType, objHit.getId());
                                objUpdateRequest.script(new Script(strScript));

                                objBulkProcessor.add(objUpdateRequest);
                            }
                        }

                        objBulkProcessor.flush();
                        objBulkProcessor.awaitClose(10l, TimeUnit.MINUTES);
                    } catch (Exception objEx) {
                        bIsFinish = false;
                        objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));

                        break;
                    }

                    objLogger.debug("Cur Hit: " + lCurNumHit);
                    objLogger.debug("Total Hits: " + lTotalHit);

                    //3. Continue to scroll
                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lScrollTTL)).get();
                } else {
                    break;
                }
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);
        }

        //3. Return
        return bIsFinish;
    }

    private void writeCSVHeader(FileWriter objFileWriter, List<String> lstHeader, Map<String, String> mapHeaderMapping) throws Exception {
        if (lstHeader != null && lstHeader.size() > 0) {
            List<String> lstMappingHeader = new ArrayList<>();

            for (int intCount = 0; intCount < lstHeader.size(); intCount++) {
                String strCurHeader = lstHeader.get(intCount);
                if (mapHeaderMapping.containsKey(strCurHeader)) {
                     lstMappingHeader.add(mapHeaderMapping.get(strCurHeader));
                } else {
                    lstMappingHeader.add(strCurHeader);
                }
            }

            List<Object> lstObj = lstMappingHeader.stream().collect(Collectors.toList());
            CSVUtil.writeLine(objFileWriter, lstObj);
        }
    }

    private void writeCSVHeader(FileWriter objFileWriter, SearchHit objSearchHit) throws Exception {
        String strSource = objSearchHit.getSourceAsString();

        if (strSource != null && !strSource.isEmpty()) {
            HashMap<String, Object> mapSource = objMapper.readValue(strSource, HashMap.class);

            if (mapSource != null && mapSource.size() > 0) {
                List<Object> lstHeader = Arrays
                        .asList(mapSource.keySet().toArray(new Object[mapSource.keySet().size()]));

                lstHeader = lstHeader.stream().map(str -> str.toString().replace("-", ".")).collect(Collectors.toList());
                CSVUtil.writeLine(objFileWriter, lstHeader);
            }
        }
    }

    private void writeESDataOfHit(FileWriter objFileWriter, SearchHit objSearchHit, Boolean bIsIncludeHeader, Integer intCount) throws Exception {
        String strSource = objSearchHit.getSourceAsString();

        if (strSource != null && !strSource.isEmpty()) {
            HashMap<String, Object> mapSource = objMapper.readValue(strSource, HashMap.class);

            if (mapSource != null && mapSource.size() > 0) {
                if (intCount == 0 && bIsIncludeHeader) {
                    List<Object> lstHeader = Arrays
                            .asList(mapSource.keySet().toArray(new Object[mapSource.keySet().size()]));

                    lstHeader = lstHeader.stream().map(str -> str.toString().replace("-", ".")).collect(Collectors.toList());
                    CSVUtil.writeLine(objFileWriter, lstHeader);

                    objLogger.debug("INFO: " + Arrays.toString(lstHeader.toArray()));
                }

                List<Object> lstValue = Arrays
                        .asList(mapSource.values().toArray(new Object[mapSource.values().size()]));
                CSVUtil.writeLine(objFileWriter, lstValue);
            }
        }
    }

    protected Boolean writeESDataToCSVFile(FileWriter objFileWriter, SearchResponse objSearchResponse,
                                           Boolean bIsIncludeHeader) {
        Boolean bIsWrote = true;

        try {
            Integer intCount = 0;

            for (SearchHit objCurHit : objSearchResponse.getHits().getHits()) {
                writeESDataOfHit(objFileWriter, objCurHit, bIsIncludeHeader, intCount);
                intCount++;
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
            bIsWrote = false;
        }

        return bIsWrote;
    }

    private Boolean writeESDetailDataToCSVFile(FileWriter objFileWriter, SearchHit[] arrDetailHit,
                                               String strDetailJoinField, Map<String, String> mapMasterData, List<String> lstDetailField) {
        Boolean bIsWrote = true;

        try {
            Integer intCount = 0;

            for (SearchHit objCurHit : arrDetailHit) {
                writeESDetailHitData(objFileWriter, objCurHit, intCount, strDetailJoinField, mapMasterData, lstDetailField);

                intCount++;
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
            bIsWrote = false;
        }

        return bIsWrote;
    }

    private void writeESDetailHitData(FileWriter objFileWriter, SearchHit objSearchHit, Integer intCount,
                                      String strDetailJoinField, Map<String, String> mapMasterData, List<String> lstDetailField) throws Exception {
        Map<String, Object> mapSource = objSearchHit.getSourceAsMap();

        if (mapSource != null && mapSource.size() > 0 && mapSource.containsKey(strDetailJoinField)) {
            mapSource.put(strDetailJoinField, mapMasterData.get(mapSource.get(strDetailJoinField).toString()));

            List<Object> lstWriteData = new ArrayList<>();

            if (!lstDetailField.contains(strDetailJoinField)) {
                lstWriteData.add(mapSource.get(strDetailJoinField));
            }

            for (int intCountField = 0; intCountField < lstDetailField.size(); intCountField++) {
                if (mapSource.containsKey(lstDetailField.get(intCountField))) {
                    lstWriteData.add(mapSource.get(lstDetailField.get(intCountField)).toString());
                } else {
                    lstWriteData.add("");
                }
            }

            CSVUtil.writeLine(objFileWriter, lstWriteData);
        }
    }

    private Boolean writeESDataToCSVFile(FileWriter objFileWriter, SearchHit[] arrSearchHit,
                                         Boolean bIsIncludeHeader) {
        Boolean bIsWrote = true;

        try {
            Integer intCount = 0;

            for (SearchHit objCurHit : arrSearchHit) {
                writeESDataOfHit(objFileWriter, objCurHit, bIsIncludeHeader, intCount);

                intCount++;
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
            bIsWrote = false;
        }

        return bIsWrote;
    }

    public String exportESDataToCSVUnder10000(String strIndex, String strType, String strFileName,
                                              Integer intPageSize) {
        Boolean bIsExported = true;

        try {
            if (objESClient != null) {
                if (new File(strFileName).exists()) {
                    new File(strFileName).delete();
                }

                File objFileName = new File(strFileName);
                File objDir = objFileName.getParentFile();

                if (!objDir.exists()) {
                    objDir.mkdirs();
                }

                new File(strFileName).createNewFile();

                FileWriter objFileWriter = new FileWriter(strFileName, true);

                SearchResponse objSearchResponse = objESFilter.searchESWithPaging(strIndex, strType, 0, intPageSize);

                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0) {
                    Long lTotalHit = objSearchResponse.getHits().getTotalHits();

                    if (lTotalHit > 0) {
                        writeESDataToCSVFile(objFileWriter, objSearchResponse, true);

                        if (lTotalHit > intPageSize) {
                            Long lCount = 0L;

                            if (lTotalHit % intPageSize == 0) {
                                lCount = lTotalHit / intPageSize - 1;
                            } else {
                                lCount = lTotalHit / intPageSize;
                            }

                            for (long i = 0; i < lCount; i++) {
                                SearchResponse objNextSearchResponse = objESFilter.searchESWithPaging(strIndex, strType,
                                        (int) ((i + 1) * intPageSize), intPageSize);

                                Boolean bIsWrote = writeESDataToCSVFile(objFileWriter, objNextSearchResponse, false);

                                if (!bIsWrote) {
                                    bIsExported = false;
                                    break;
                                }
                            }
                        }
                    }
                }

                objFileWriter.flush();
                objFileWriter.close();
            }
        } catch (Exception objEx) {
            bIsExported = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        if (bIsExported) {
            return strFileName;
        } else {
            return "";
        }
    }

    private List<ESFileModel> exportESDataToCSV(String strIndexPattern, String strType, HashMap<String, String> mapTypeIndex, String strFilePattern,
                                                Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest,
                                                Boolean bIsMultipleFile, Integer intMaxFileLine) {
        List<ESFileModel> lstCSVFile = new ArrayList<>();

        if (mapTypeIndex == null || mapTypeIndex.size() <= 0) {
            mapTypeIndex = new HashMap<String, String>();
        }

        List<String> lstAllFile = new ArrayList<>();

        Calendar objBegin = Calendar.getInstance();

        if (strIndexPattern != null && strType != null && !strIndexPattern.isEmpty() && !strType.isEmpty()) {
            lstAllFile = exportPatternESDataToCSV(strIndexPattern, strType, strFilePattern, intPageSize,
                    objFilterAllRequest, bIsMultipleFile, intMaxFileLine);
        } else {
            for (Map.Entry<String, String> curTypeIndex : mapTypeIndex.entrySet()) {
                List<String> lstCurFile = exportPatternESDataToCSV(curTypeIndex.getKey(), curTypeIndex.getValue(),
                        strFilePattern.replace(".", "_" + curTypeIndex.getKey() + "."), intPageSize,
                        objFilterAllRequest, bIsMultipleFile, intMaxFileLine);

                if (lstCurFile != null && lstCurFile.size() > 0) {
                    lstAllFile.addAll(lstCurFile);
                }
            }
        }

        Long lElapsedTime = Calendar.getInstance().getTimeInMillis() - objBegin.getTimeInMillis();

        if (lstAllFile != null && lstAllFile.size() > 0) {
            for (int intCount = 0; intCount < lstAllFile.size(); intCount++) {
                File objFile = new File(lstAllFile.get(intCount));

                if (objFile.exists()) {
                    ESFileModel objCurFileModel = new ESFileModel();
                    objCurFileModel.setFile_name(objFile.getName());
                    objCurFileModel.setFile_path(objFile.getAbsolutePath());
                    objCurFileModel.setFile_size(objFile.length());
                    objCurFileModel.setProcessed_time(lElapsedTime);

                    lstCSVFile.add(objCurFileModel);
                }
            }
        }

        return lstCSVFile;
    }

    private List<String> exportPatternESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest, Boolean bIsMultipleFile, Integer intMaxFileLine) {
        Boolean bIsExported = true;
        List<String> lstExportedFile = new ArrayList<>();

        try {
            if (objESClient != null) {
                String strNewFile = FileUtil.createFile(strFileName);

                if (strNewFile != null && !strNewFile.isEmpty()) {
                    FileWriter objFileWriter = new FileWriter(strNewFile, true);

                    //Refresh index before export
                    objESConnection.refreshIndex(strIndex);

                    List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);

                    List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                            && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                            ? objFilterAllRequest.getFilters()
                            : new ArrayList<ESFilterRequestModel>();

                    Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

                    SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

                    if (lstFilters != null && lstFilters.size() > 0) {
                        List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                        BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                        List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                        if (objQueryBuilder != null) {
                            // Special case: to get value from UCL, LCL, must get statistic information first
                            if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                                objQueryBuilder = objESFilter.generateAggQueryBuilder(strIndex, strType, objQueryBuilder,
                                        lstNotAddedFilterRequest, lstFieldModel);
                            }

                            objSearchSourceBuilder.query(objQueryBuilder);
                        }
                    }

                    SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                            .setSource(objSearchSourceBuilder)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                            .setSize(intPageSize).get();

                    Integer intCurHitLine = 0;
                    Integer intLastWriteLine = 0;
                    Boolean bIsFirstWrite = true;
                    Integer intCurNumFile = 0;

                    Boolean bIsWriteCSV = false;
                    Integer intCurSkipLine = 0;

                    do {
                        if (objSearchResponse != null && objSearchResponse.getHits() != null
                                && objSearchResponse.getHits().getTotalHits() > 0
                                && objSearchResponse.getHits().getHits() != null
                                && objSearchResponse.getHits().getHits().length > 0) {
                            if (bIsFirstWrite) {
                                bIsFirstWrite = false;
                                writeCSVHeader(objFileWriter, objSearchResponse.getHits().getHits()[0]);
                            }

                            if (bIsMultipleFile && intMaxFileLine > 0) {
                                intCurHitLine = objSearchResponse.getHits().getHits().length;
                                Integer intCurTotalLine = intLastWriteLine + intCurHitLine;

                                if (intCurTotalLine >= intMaxFileLine) {
                                    Boolean bIsContinue = false;

                                    do {
                                        Integer intNeedToWrite = intMaxFileLine - intLastWriteLine;
                                        List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).limit(intNeedToWrite).collect(Collectors.toList());

                                        bIsWriteCSV = writeESDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), false);

                                        objFileWriter.flush();
                                        objFileWriter.close();

                                        lstExportedFile.add(strNewFile);

                                        if (bIsWriteCSV) {
                                            strNewFile = strFileName.replace(".", "_" + intCurNumFile.toString() + ".");
                                            strNewFile = FileUtil.createFile(strNewFile);
                                            objFileWriter = new FileWriter(strNewFile, true);
                                            writeCSVHeader(objFileWriter, objSearchResponse.getHits().getHits()[0]);

                                            intCurNumFile += 1;
                                            intLastWriteLine = 0;
                                            intCurSkipLine += lstHit.size();

                                            List<SearchHit> lstRemainHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());

                                            if (lstRemainHit.size() >= intMaxFileLine) {
                                                bIsContinue = false;
                                            } else {
                                                bIsContinue = true;

                                                if (lstRemainHit != null && lstRemainHit.size() > 0) {
                                                    intLastWriteLine = lstRemainHit.size();
                                                    intCurSkipLine = lstRemainHit.size();

                                                    bIsWriteCSV = writeESDataToCSVFile(objFileWriter, lstRemainHit.toArray(new SearchHit[lstRemainHit.size()]), false);
                                                } else {
                                                    intLastWriteLine = 0;
                                                    intCurSkipLine = 0;
                                                }
                                            }
                                        } else {
                                            throw new Exception();
                                        }
                                    } while (!bIsContinue);
                                } else {
                                    List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());
                                    bIsWriteCSV = writeESDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), false);
                                }
                            } else {
                                bIsWriteCSV = writeESDataToCSVFile(objFileWriter, objSearchResponse, false);
                            }

                            if (!bIsWriteCSV) {
                                bIsExported = false;
                                break;
                            }
                        }

                        objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                .setScroll(new TimeValue(lScrollTTL)).get();
                    } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0);

                    if (objFileWriter != null) {
                        objFileWriter.flush();
                        objFileWriter.close();

                        lstExportedFile.add(strNewFile);
                    }
                }
            }
        } catch (Exception objEx) {
            bIsExported = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        if (bIsExported) {
            return lstExportedFile;
        } else {
            return Arrays.asList("");
        }
    }

    public List<ESFileModel> exportESMasterDetailDataToCSV(String strMasterIndex, String strMasterType,
                                                           String strDetailIndex, String strDetailType,
                                                           String strMasterJoinField, String strDetailJoinField, Integer intPageSize,
                                                           List<String> lstPredefineHeader, HashMap<String, String> mapDateField,
                                                           ESFilterAllRequestModel objFilterAllRequest, String strFileName,
                                                           Boolean bIsMultipleFile, Integer intMaxFileLine,
                                                           Map<String, String> mapHeaderMapping) {
        Boolean bIsExported = true;
        List<ESFileModel> lstReturnFile = new ArrayList<>();
        List<String> lstExportedFile = new ArrayList<>();

        Calendar objBegin = Calendar.getInstance();

        try {
            Map<String, SimpleDateFormat> mapDateFormat = new HashMap<>();

            if (mapDateField != null && mapDateField.size() > 0) {
                for (Map.Entry<String, String> curDateField: mapDateField.entrySet()) {
                    SimpleDateFormat objCurDateFormat = new SimpleDateFormat(curDateField.getValue());
                    mapDateFormat.put(curDateField.getKey(), objCurDateFormat);
                }
            }
            //- Get all data from master index with filter
            //- Generate Master CSV String Format map with key: join field
            //  - Master CSV Header
            //  - Master CSV Data Map (join_field -> csv string)

            //Refresh index before export
            List<String> lstMasterHeader = new ArrayList<>();
            Map<String, String> mapMasterData = new HashMap<>();

            objESConnection.refreshIndex(strMasterIndex);

            List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strMasterIndex, strMasterType, null, false);

            List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                    && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                    ? objFilterAllRequest.getFilters()
                    : new ArrayList<ESFilterRequestModel>();

            Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

            SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

            if (lstFilters != null && lstFilters.size() > 0) {
                List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                if (objQueryBuilder != null) {
                    if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                        objQueryBuilder = objESFilter.generateAggQueryBuilder(strMasterIndex, strMasterType, objQueryBuilder,
                                lstNotAddedFilterRequest, lstFieldModel);
                    }

                    objSearchSourceBuilder.query(objQueryBuilder);
                }

                lstMasterHeader = lstFieldModel.stream().map(curItem -> curItem.getFull_name()).collect(Collectors.toList());

                if (lstPredefineHeader != null && lstPredefineHeader.size() > 0) {
                    lstMasterHeader = lstMasterHeader.stream().filter(curHeader -> lstPredefineHeader.contains(curHeader)).collect(Collectors.toList());
                }
            }

            SearchResponse objSearchResponse = objESClient.prepareSearch(strMasterIndex).setTypes(strMasterType)
                    .setSource(objSearchSourceBuilder)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intPageSize).get();

            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    for (SearchHit objCurHit : objSearchResponse.getHits().getHits()) {
                        Map<String, Object> mapCurHit = objCurHit.getSourceAsMap();

                        if (mapCurHit.containsKey(strMasterJoinField)) {
                            String strCurJoinID = mapCurHit.get(strMasterJoinField).toString();
                            List<String> lstCurHit = new ArrayList<>();

                            for (int intCount = 0; intCount < lstMasterHeader.size(); intCount++) {
                                String strCurField = lstMasterHeader.get(intCount);

                                if (mapCurHit.containsKey(strCurField)) {
                                    if (mapDateFormat != null && mapDateFormat.containsKey(strCurField)) {
                                        try {
                                            String strCurDate = mapDateFormat.get(strCurField).format(new Date(Long.valueOf(mapCurHit.get(strCurField).toString())));

                                            if (strCurDate != null && !strCurDate.isEmpty()) {
                                                lstCurHit.add(strCurDate);
                                            } else {
                                                lstCurHit.add(mapCurHit.get(strCurField).toString());
                                            }
                                        } catch (Exception objEx) {
                                            lstCurHit.add(mapCurHit.get(strCurField).toString());
                                        }
                                    } else {
                                        lstCurHit.add(mapCurHit.get(strCurField).toString());
                                    }
                                } else {
                                    lstCurHit.add("");
                                }
                            }

                            mapMasterData.put(strCurJoinID, Strings.join(lstCurHit, ","));
                        }
                    }
                }

                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                        .setScroll(new TimeValue(lScrollTTL)).get();
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);

            //- Create filter on data index with list of master join field from master data
            BoolQueryBuilder objDetailQueryBuilder = new BoolQueryBuilder();

            for (Map.Entry<String, String> curHeader : mapMasterData.entrySet()) {
                TermQueryBuilder objHeaderQueryBuilder = QueryBuilders.termQuery(strDetailJoinField, curHeader.getKey());
                objDetailQueryBuilder.should(objHeaderQueryBuilder);
            }

            //- Scroll on data index to query data and write to CSV files
            objSearchSourceBuilder = new SearchSourceBuilder();
            objSearchSourceBuilder.query(objDetailQueryBuilder);

            String strNewFile = FileUtil.createFile(strFileName);

            if (strNewFile != null && !strNewFile.isEmpty()) {
                FileWriter objFileWriter = new FileWriter(strNewFile, true);

                objESConnection.refreshIndex(strDetailIndex);
                lstFieldModel = objESConnection.getFieldsMetaData(strDetailIndex, strDetailType, null, false);

                List<String> lstDetailHeader = new ArrayList<>();

                if (lstFieldModel != null && lstFieldModel.size() > 0) {
                    lstDetailHeader = lstFieldModel.stream().map(curItem -> curItem.getFull_name()).collect(Collectors.toList());

                    if (lstPredefineHeader != null && lstPredefineHeader.size() > 0) {
                        lstDetailHeader = lstDetailHeader.stream().filter(curHeader -> lstPredefineHeader.contains(curHeader)).collect(Collectors.toList());
                    }

                    List<String> lstMergeHeader = new ArrayList<>();

                    if (lstDetailHeader.contains(strDetailJoinField)) {
                        lstDetailHeader.remove(strDetailJoinField);
                        lstMasterHeader.remove(strMasterJoinField);

                        for (int intCount = 0; intCount < lstMasterHeader.size(); intCount++) {
                            if (lstDetailHeader.contains(lstMasterHeader.get(intCount))) {
                                lstDetailHeader.remove(lstMasterHeader.get(intCount));
                            }
                        }

                        List<String> lstOrderDetailHeader = new ArrayList<>();
                        List<String> lstRemainDetainHeader = new ArrayList<>();

                        for (int intCount = 0; intCount < lstPredefineHeader.size(); intCount++) {
                            if (lstDetailHeader.contains(lstPredefineHeader.get(intCount))) {
                                lstOrderDetailHeader.add(lstPredefineHeader.get(intCount));
                            }
                        }

                        lstRemainDetainHeader = lstDetailHeader.stream().filter(curItem -> !lstOrderDetailHeader.contains(curItem)).collect(Collectors.toList());
                        lstOrderDetailHeader.addAll(lstRemainDetainHeader);

                        lstDetailHeader = lstOrderDetailHeader;

                        lstMergeHeader = new ArrayList<>(Arrays.asList(strDetailJoinField));
                        lstMergeHeader.addAll(lstMasterHeader);
                        lstMergeHeader.addAll(lstDetailHeader);

                        objSearchResponse = objESClient.prepareSearch(strDetailIndex).setTypes(strDetailType)
                                .setSource(objSearchSourceBuilder)
                                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                                .setSize(intPageSize).get();

                        Integer intCurHitLine = 0;
                        Integer intLastWriteLine = 0;
                        Boolean bIsFirstWrite = true;
                        Integer intCurNumFile = 0;

                        Boolean bIsWriteCSV = false;
                        Integer intCurSkipLine = 0;
                        Integer intNumLoop = 0;

                        do {
                            if (objSearchResponse != null && objSearchResponse.getHits() != null
                                    && objSearchResponse.getHits().getTotalHits() > 0
                                    && objSearchResponse.getHits().getHits() != null
                                    && objSearchResponse.getHits().getHits().length > 0) {
                                intNumLoop += 1;
                                Calendar dtCurHitTime = Calendar.getInstance();

                                if (bIsFirstWrite) {
                                    bIsFirstWrite = false;
                                    writeCSVHeader(objFileWriter, lstMergeHeader, mapHeaderMapping);
                                }

                                if (bIsMultipleFile && intMaxFileLine > 0) {
                                    intCurHitLine = objSearchResponse.getHits().getHits().length;
                                    Integer intCurTotalLine = intLastWriteLine + intCurHitLine;

                                    if (intCurTotalLine >= intMaxFileLine) {
                                        Boolean bIsContinue = false;

                                        do {
                                            Integer intNeedToWrite = intMaxFileLine - intLastWriteLine;
                                            List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).limit(intNeedToWrite).collect(Collectors.toList());

                                            bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);

                                            objFileWriter.flush();
                                            objFileWriter.close();

                                            lstExportedFile.add(strNewFile);

                                            if (bIsWriteCSV) {
                                                strNewFile = strFileName.replace(".", "_" + intCurNumFile.toString() + ".");
                                                strNewFile = FileUtil.createFile(strNewFile);
                                                objFileWriter = new FileWriter(strNewFile, true);
                                                writeCSVHeader(objFileWriter, objSearchResponse.getHits().getHits()[0]);

                                                intCurNumFile += 1;
                                                intLastWriteLine = 0;
                                                intCurSkipLine += lstHit.size();

                                                List<SearchHit> lstRemainHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());

                                                if (lstRemainHit.size() >= intMaxFileLine) {
                                                    bIsContinue = false;
                                                } else {
                                                    bIsContinue = true;

                                                    if (lstRemainHit != null && lstRemainHit.size() > 0) {
                                                        intLastWriteLine = lstRemainHit.size();
                                                        intCurSkipLine = lstRemainHit.size();

                                                        bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);
                                                    } else {
                                                        intLastWriteLine = 0;
                                                        intCurSkipLine = 0;
                                                    }
                                                }
                                            } else {
                                                throw new Exception();
                                            }
                                        } while (!bIsContinue);
                                    } else {
                                        List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());
                                        bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);
                                    }
                                } else {
                                    bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, objSearchResponse.getHits().getHits(), strDetailJoinField, mapMasterData, lstDetailHeader);
                                }

                                if (!bIsWriteCSV) {
                                    bIsExported = false;
                                    break;
                                }

                                objLogger.debug("Loop: " + intNumLoop + " Hit: " + objSearchResponse.getHits().getHits().length + " - Time: " + (Calendar.getInstance().getTimeInMillis() - dtCurHitTime.getTimeInMillis()) / 1000.0 + "secs");
                            }

                            objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                    .setScroll(new TimeValue(lScrollTTL)).get();
                        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                                && objSearchResponse.getHits().getHits() != null
                                && objSearchResponse.getHits().getHits().length > 0);

                        if (objFileWriter != null) {
                            objFileWriter.flush();
                            objFileWriter.close();

                            lstExportedFile.add(strNewFile);
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("WARN: " + ExceptionUtil.getStackTrace(objEx));
        }

        Long lElapsedTime = Calendar.getInstance().getTimeInMillis() - objBegin.getTimeInMillis();

        if (lstExportedFile != null && lstExportedFile.size() > 0) {
            for (int intCount = 0; intCount < lstExportedFile.size(); intCount++) {
                File objFile = new File(lstExportedFile.get(intCount));

                if (objFile.exists()) {
                    ESFileModel objCurFileModel = new ESFileModel();
                    objCurFileModel.setFile_name(objFile.getName());
                    objCurFileModel.setFile_path(objFile.getAbsolutePath());
                    objCurFileModel.setFile_size(objFile.length());
                    objCurFileModel.setProcessed_time(lElapsedTime);

                    lstReturnFile.add(objCurFileModel);
                }
            }
        }

        return lstReturnFile;
    }

    public List<ESFileModel> exportESMasterDetailDataToCSVWithMasterDetailFilter(String strMasterIndex, String strMasterType,
                                                                                 String strDetailIndex, String strDetailType,
                                                                                 String strMasterJoinField, String strDetailJoinField, Integer intPageSize,
                                                                                 List<String> lstPredefineHeader, HashMap<String, String> mapDateField,
                                                                                 ESFilterAllRequestModel objFilterMasterRequest,
                                                                                 ESFilterAllRequestModel objFilterDetailRequest,
                                                                                 String strFileName,
                                                                                 Boolean bIsMultipleFile, Integer intMaxFileLine,
                                                                                 Map<String, String> mapHeaderMapping) {
        Boolean bIsExported = true;
        List<ESFileModel> lstReturnFile = new ArrayList<>();
        List<String> lstExportedFile = new ArrayList<>();

        Calendar objBegin = Calendar.getInstance();

        try {
            Map<String, SimpleDateFormat> mapDateFormat = new HashMap<>();

            if (mapDateField != null && mapDateField.size() > 0) {
                for (Map.Entry<String, String> curDateField: mapDateField.entrySet()) {
                    SimpleDateFormat objCurDateFormat = new SimpleDateFormat(curDateField.getValue());
                    mapDateFormat.put(curDateField.getKey(), objCurDateFormat);
                }
            }
            //- Get all data from master index with filter
            //- Generate Master CSV String Format map with key: join field
            //  - Master CSV Header
            //  - Master CSV Data Map (join_field -> csv string)

            //Refresh index before export
            List<String> lstMasterHeader = new ArrayList<>();
            Map<String, String> mapMasterData = new HashMap<>();

            objESConnection.refreshIndex(strMasterIndex);

            List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strMasterIndex, strMasterType, null, false);

            List<ESFilterRequestModel> lstFilters = (objFilterMasterRequest != null
                    && objFilterMasterRequest.getFilters() != null && objFilterMasterRequest.getFilters().size() > 0)
                    ? objFilterMasterRequest.getFilters()
                    : new ArrayList<ESFilterRequestModel>();

            Boolean bIsReversedFilter = (objFilterMasterRequest != null && objFilterMasterRequest.getIs_reversed() != null) ? objFilterMasterRequest.getIs_reversed() : false;

            SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

            if (lstFilters != null && lstFilters.size() > 0) {
                List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                if (objQueryBuilder != null) {
                    if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                        objQueryBuilder = objESFilter.generateAggQueryBuilder(strMasterIndex, strMasterType, objQueryBuilder,
                                lstNotAddedFilterRequest, lstFieldModel);
                    }

                    objSearchSourceBuilder.query(objQueryBuilder);
                }

                lstMasterHeader = lstFieldModel.stream().map(curItem -> curItem.getFull_name()).collect(Collectors.toList());

                if (lstPredefineHeader != null && lstPredefineHeader.size() > 0) {
                    List<String> lstOrderedHeader = new ArrayList<>();
                    lstMasterHeader = lstMasterHeader.stream().filter(curHeader -> lstPredefineHeader.contains(curHeader)).collect(Collectors.toList());

                    for (int intCount = 0; intCount < lstPredefineHeader.size(); intCount++) {
                        if (lstMasterHeader.contains(lstPredefineHeader.get(intCount))) {
                            lstOrderedHeader.add(lstPredefineHeader.get(intCount));
                        }
                    }
                    lstMasterHeader = lstOrderedHeader;
                }
            }

            SearchResponse objSearchResponse = objESClient.prepareSearch(strMasterIndex).setTypes(strMasterType)
                    .setSource(objSearchSourceBuilder)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intPageSize).get();

            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    for (SearchHit objCurHit : objSearchResponse.getHits().getHits()) {
                        Map<String, Object> mapCurHit = objCurHit.getSourceAsMap();

                        if (mapCurHit.containsKey(strMasterJoinField)) {
                            String strCurJoinID = mapCurHit.get(strMasterJoinField).toString();
                            List<String> lstCurHit = new ArrayList<>();

                            for (int intCount = 0; intCount < lstMasterHeader.size(); intCount++) {
                                String strCurField = lstMasterHeader.get(intCount);

                                if (mapCurHit.containsKey(strCurField)) {
                                    if (mapDateFormat != null && mapDateFormat.containsKey(strCurField)) {
                                        try {
                                            String strCurDate = mapDateFormat.get(strCurField).format(new Date(Long.valueOf(mapCurHit.get(strCurField).toString())));

                                            if (strCurDate != null && !strCurDate.isEmpty()) {
                                                lstCurHit.add(strCurDate);
                                            } else {
                                                lstCurHit.add(mapCurHit.get(strCurField).toString());
                                            }
                                        } catch (Exception objEx) {
                                            lstCurHit.add(mapCurHit.get(strCurField).toString());
                                        }
                                    } else {
                                        lstCurHit.add(mapCurHit.get(strCurField).toString());
                                    }
                                } else {
                                    lstCurHit.add("");
                                }
                            }

                            mapMasterData.put(strCurJoinID, Strings.join(lstCurHit, ","));
                        }
                    }
                }

                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                        .setScroll(new TimeValue(lScrollTTL)).get();
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);

            //- Create filter on data index with list of master join field from master data
            BoolQueryBuilder objDetailQueryBuilder = new BoolQueryBuilder();

            BoolQueryBuilder objSubDetailQueryBuilder = new BoolQueryBuilder();

            for (Map.Entry<String, String> curHeader : mapMasterData.entrySet()) {
                TermQueryBuilder objHeaderQueryBuilder = QueryBuilders.termQuery(strDetailJoinField, curHeader.getKey());
                objSubDetailQueryBuilder.should(objHeaderQueryBuilder);
            }

            objDetailQueryBuilder.must(objSubDetailQueryBuilder);

            //TODO Apply detail filter on detail index
            lstFieldModel = objESConnection.getFieldsMetaData(strDetailIndex, strDetailType, null, false);

            lstFilters = (objFilterDetailRequest != null
                    && objFilterDetailRequest.getFilters() != null && objFilterDetailRequest.getFilters().size() > 0)
                    ? objFilterDetailRequest.getFilters()
                    : new ArrayList<ESFilterRequestModel>();

            bIsReversedFilter = (objFilterDetailRequest != null && objFilterDetailRequest.getIs_reversed() != null) ? objFilterDetailRequest.getIs_reversed() : false;

            if (lstFilters != null && lstFilters.size() > 0) {
                List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                if (objQueryBuilder != null) {
                    if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                        objQueryBuilder = objESFilter.generateAggQueryBuilder(strDetailIndex, strDetailType, objQueryBuilder,
                                lstNotAddedFilterRequest, lstFieldModel);
                    }

                    objDetailQueryBuilder.must(objQueryBuilder);
                }
            }

            //- Scroll on data index to query data and write to CSV files
            objSearchSourceBuilder = new SearchSourceBuilder();
            objSearchSourceBuilder.query(objDetailQueryBuilder);

            String strNewFile = FileUtil.createFile(strFileName);

            if (strNewFile != null && !strNewFile.isEmpty()) {
                FileWriter objFileWriter = new FileWriter(strNewFile, true);

                objESConnection.refreshIndex(strDetailIndex);
                lstFieldModel = objESConnection.getFieldsMetaData(strDetailIndex, strDetailType, null, false);

                List<String> lstDetailHeader = new ArrayList<>();

                if (lstFieldModel != null && lstFieldModel.size() > 0) {
                    lstDetailHeader = lstFieldModel.stream().map(curItem -> curItem.getFull_name()).collect(Collectors.toList());

                    if (lstPredefineHeader != null && lstPredefineHeader.size() > 0) {
                        lstDetailHeader = lstDetailHeader.stream().filter(curHeader -> lstPredefineHeader.contains(curHeader)).collect(Collectors.toList());
                    }

                    List<String> lstMergeHeader = new ArrayList<>();

                    if (lstDetailHeader.contains(strDetailJoinField)) {
                        lstDetailHeader.remove(strDetailJoinField);
                        lstMasterHeader.remove(strMasterJoinField);

                        for (int intCount = 0; intCount < lstMasterHeader.size(); intCount++) {
                            if (lstDetailHeader.contains(lstMasterHeader.get(intCount))) {
                                lstDetailHeader.remove(lstMasterHeader.get(intCount));
                            }
                        }

                        lstMergeHeader = new ArrayList<>(Arrays.asList(strDetailJoinField));
                        lstMergeHeader.addAll(lstMasterHeader);
                        lstMergeHeader.addAll(lstDetailHeader);

                        objSearchResponse = objESClient.prepareSearch(strDetailIndex).setTypes(strDetailType)
                                .setSource(objSearchSourceBuilder)
                                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                                .setSize(intPageSize).get();

                        Integer intCurHitLine = 0;
                        Integer intLastWriteLine = 0;
                        Boolean bIsFirstWrite = true;
                        Integer intCurNumFile = 0;

                        Boolean bIsWriteCSV = false;
                        Integer intCurSkipLine = 0;
                        Integer intNumLoop = 0;

                        do {
                            if (objSearchResponse != null && objSearchResponse.getHits() != null
                                    && objSearchResponse.getHits().getTotalHits() > 0
                                    && objSearchResponse.getHits().getHits() != null
                                    && objSearchResponse.getHits().getHits().length > 0) {
                                intNumLoop += 1;
                                Calendar dtCurHitTime = Calendar.getInstance();

                                if (bIsFirstWrite) {
                                    bIsFirstWrite = false;
                                    writeCSVHeader(objFileWriter, lstMergeHeader.stream().collect(Collectors.toList()), mapHeaderMapping);
                                }

                                if (bIsMultipleFile && intMaxFileLine > 0) {
                                    intCurHitLine = objSearchResponse.getHits().getHits().length;
                                    Integer intCurTotalLine = intLastWriteLine + intCurHitLine;

                                    if (intCurTotalLine >= intMaxFileLine) {
                                        Boolean bIsContinue = false;

                                        do {
                                            Integer intNeedToWrite = intMaxFileLine - intLastWriteLine;
                                            List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).limit(intNeedToWrite).collect(Collectors.toList());

                                            bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);

                                            objFileWriter.flush();
                                            objFileWriter.close();

                                            lstExportedFile.add(strNewFile);

                                            if (bIsWriteCSV) {
                                                strNewFile = strFileName.replace(".", "_" + intCurNumFile.toString() + ".");
                                                strNewFile = FileUtil.createFile(strNewFile);
                                                objFileWriter = new FileWriter(strNewFile, true);
                                                writeCSVHeader(objFileWriter, objSearchResponse.getHits().getHits()[0]);

                                                intCurNumFile += 1;
                                                intLastWriteLine = 0;
                                                intCurSkipLine += lstHit.size();

                                                List<SearchHit> lstRemainHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());

                                                if (lstRemainHit.size() >= intMaxFileLine) {
                                                    bIsContinue = false;
                                                } else {
                                                    bIsContinue = true;

                                                    if (lstRemainHit != null && lstRemainHit.size() > 0) {
                                                        intLastWriteLine = lstRemainHit.size();
                                                        intCurSkipLine = lstRemainHit.size();

                                                        bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);
                                                    } else {
                                                        intLastWriteLine = 0;
                                                        intCurSkipLine = 0;
                                                    }
                                                }
                                            } else {
                                                throw new Exception();
                                            }
                                        } while (!bIsContinue);
                                    } else {
                                        List<SearchHit> lstHit = Arrays.asList(objSearchResponse.getHits().getHits()).stream().skip(intCurSkipLine).collect(Collectors.toList());
                                        bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, lstHit.toArray(new SearchHit[lstHit.size()]), strDetailJoinField, mapMasterData, lstDetailHeader);
                                    }
                                } else {
                                    bIsWriteCSV = writeESDetailDataToCSVFile(objFileWriter, objSearchResponse.getHits().getHits(), strDetailJoinField, mapMasterData, lstDetailHeader);
                                }

                                if (!bIsWriteCSV) {
                                    bIsExported = false;
                                    break;
                                }

                                objLogger.debug("Loop: " + intNumLoop + " Hit: " + objSearchResponse.getHits().getHits().length + " - Time: " + (Calendar.getInstance().getTimeInMillis() - dtCurHitTime.getTimeInMillis()) / 1000.0 + "secs");
                            }

                            objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                    .setScroll(new TimeValue(lScrollTTL)).get();
                        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                                && objSearchResponse.getHits().getHits() != null
                                && objSearchResponse.getHits().getHits().length > 0);

                        if (objFileWriter != null) {
                            objFileWriter.flush();
                            objFileWriter.close();

                            lstExportedFile.add(strNewFile);
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error(ExceptionUtil.getStackTrace(objEx));
        }

        Long lElapsedTime = Calendar.getInstance().getTimeInMillis() - objBegin.getTimeInMillis();

        if (lstExportedFile != null && lstExportedFile.size() > 0) {
            for (int intCount = 0; intCount < lstExportedFile.size(); intCount++) {
                File objFile = new File(lstExportedFile.get(intCount));

                if (objFile.exists()) {
                    ESFileModel objCurFileModel = new ESFileModel();
                    objCurFileModel.setFile_name(objFile.getName());
                    objCurFileModel.setFile_path(objFile.getAbsolutePath());
                    objCurFileModel.setFile_size(objFile.length());
                    objCurFileModel.setProcessed_time(lElapsedTime);

                    lstReturnFile.add(objCurFileModel);
                }
            }
        }

        return lstReturnFile;
    }

    public List<ESFileModel> exportESTransposeDataToCSV(String strMasterIndex, String strMasterType,
                                                        String strTransposeIndex, String strTransposeType,
                                                        String strMasterJoinField, String strTransposeJoinField,
                                                        List<String> lstTransposeFieldName, String strFieldNameSeparator,
                                                        List<String> lstTransposeFieldValue, Integer intPageSize,
                                                        HashMap<String, String> mapDateField,
                                                        ESFilterAllRequestModel objFilterAllRequest, String strFileName) {
        Boolean bIsExported = true;
        List<ESFileModel> lstReturnFile = new ArrayList<>();
        List<String> lstExportedFile = new ArrayList<>();

        Calendar objBegin = Calendar.getInstance();

        try {
            Map<String, SimpleDateFormat> mapDateFormat = new HashMap<>();

            if (mapDateField != null && mapDateField.size() > 0) {
                for (Map.Entry<String, String> curDateField: mapDateField.entrySet()) {
                    SimpleDateFormat objCurDateFormat = new SimpleDateFormat(curDateField.getValue());
                    mapDateFormat.put(curDateField.getKey(), objCurDateFormat);
                }
            }

            //- Get all data from master index with filter
            //- Generate Master CSV String Format map with key: join field
            //  - Master CSV Header
            //  - Master CSV Data Map (join_field -> csv string)

            //Refresh index before export
            List<String> lstMasterHeader = new ArrayList<>();
            Map<String, String> mapMasterData = new HashMap<>();

            objESConnection.refreshIndex(strMasterIndex);

            List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strMasterIndex, strMasterType, null, false);

            List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                    && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                    ? objFilterAllRequest.getFilters()
                    : new ArrayList<ESFilterRequestModel>();

            Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

            SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

            if (lstFilters != null && lstFilters.size() > 0) {
                List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                if (objQueryBuilder != null) {
                    if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                        objQueryBuilder = objESFilter.generateAggQueryBuilder(strMasterIndex, strMasterType, objQueryBuilder,
                                lstNotAddedFilterRequest, lstFieldModel);
                    }

                    objSearchSourceBuilder.query(objQueryBuilder);
                }

                lstMasterHeader = lstFieldModel.stream().map(curItem -> curItem.getFull_name()).collect(Collectors.toList());
            }

            SearchResponse objSearchResponse = objESClient.prepareSearch(strMasterIndex).setTypes(strMasterType)
                    .setSource(objSearchSourceBuilder)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                    .setSize(intPageSize).get();

            do {
                if (objSearchResponse != null && objSearchResponse.getHits() != null
                        && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0) {
                    for (SearchHit objCurHit : objSearchResponse.getHits().getHits()) {
                        Map<String, Object> mapCurHit = objCurHit.getSourceAsMap();

                        if (mapCurHit.containsKey(strMasterJoinField)) {
                            String strCurJoinID = mapCurHit.get(strMasterJoinField).toString();
                            List<String> lstCurHit = new ArrayList<>();

                            for (int intCount = 0; intCount < lstMasterHeader.size(); intCount++) {
                                String strCurField = lstMasterHeader.get(intCount);
                                if (mapDateFormat != null && mapDateFormat.containsKey(strCurField)) {
                                    try {
                                        String strCurDate = mapDateFormat.get(strCurField).format(new Date(Long.valueOf(mapCurHit.get(strCurField).toString())));

                                        if (strCurDate != null && !strCurDate.isEmpty()) {
                                            lstCurHit.add(strCurDate);
                                        } else {
                                            lstCurHit.add(mapCurHit.get(strCurField).toString());
                                        }
                                    } catch (Exception objEx) {
                                        lstCurHit.add(mapCurHit.get(strCurField).toString());
                                    }
                                } else {
                                    lstCurHit.add(mapCurHit.get(strCurField).toString());
                                }
                            }

                            mapMasterData.put(strCurJoinID, Strings.join(lstCurHit, ","));
                        }
                    }
                }

                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                        .setScroll(new TimeValue(lScrollTTL)).get();
            } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0);

            //- Get data from transpose index and transposing
            List<Object> lstAllHeader = new ArrayList<>();
            List<String> lstAllParam = new ArrayList<>();
            lstAllHeader.addAll(lstMasterHeader);

            ConcurrentHashMap<String, Boolean> mapTransposeParam = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, HashMap<String, Object>> mapTransposeData = new ConcurrentHashMap<>();
            objESConnection.refreshIndex(strTransposeIndex);

            mapMasterData.keySet().parallelStream().forEach(curHeaderID -> {
                //- Create filter on data index with list of master join field from master data
                BoolQueryBuilder objDetailQueryBuilder = new BoolQueryBuilder();
                TermQueryBuilder objHeaderQueryBuilder = QueryBuilders.termQuery(strTransposeJoinField, curHeaderID);
                objDetailQueryBuilder.must(objHeaderQueryBuilder);

                //- Scroll on data index to query data and write to CSV files
                SearchSourceBuilder objTransposeSearchSourceBuilder = new SearchSourceBuilder();
                objTransposeSearchSourceBuilder.query(objDetailQueryBuilder);

                SearchResponse objTransposeSearchResponse = objESClient.prepareSearch(strTransposeIndex).setTypes(strTransposeType)
                        .setSource(objTransposeSearchSourceBuilder)
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                        .setSize(intPageSize).get();

                HashMap<String, Object> mapParamValue = new HashMap<>();

                do {
                    if (objTransposeSearchResponse != null && objTransposeSearchResponse.getHits() != null
                            && objTransposeSearchResponse.getHits().getTotalHits() > 0
                            && objTransposeSearchResponse.getHits().getHits() != null
                            && objTransposeSearchResponse.getHits().getHits().length > 0) {
                        for (SearchHit curHit : objTransposeSearchResponse.getHits().getHits()) {
                            Map<String, Object> mapCurHit = curHit.getSourceAsMap();

                            List<String> lstCurFieldNameValue = new ArrayList<>();
                            Boolean bIsAllExist = true;

                            for (int intCount = 0; intCount < lstTransposeFieldName.size(); intCount++) {
                                if (mapCurHit.containsKey(lstTransposeFieldName.get(intCount))) {
                                    lstCurFieldNameValue.add(mapCurHit.get(lstTransposeFieldName.get(intCount)).toString());
                                } else {
                                    bIsAllExist = false;
                                    break;
                                }
                            }
                            if (bIsAllExist) {
                                String strCurKey = Strings.join(lstCurFieldNameValue, strFieldNameSeparator);
                                mapTransposeParam.put(strCurKey, true);

                                for (int intCountValField = 0; intCountValField < lstTransposeFieldValue.size(); intCountValField++) {
                                    if (mapCurHit.containsKey(lstTransposeFieldValue.get(intCountValField))) {
                                        mapParamValue.put(strCurKey, mapCurHit.get(lstTransposeFieldValue.get(intCountValField)));
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    objTransposeSearchResponse = objESClient.prepareSearchScroll(objTransposeSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lScrollTTL)).get();
                } while (objTransposeSearchResponse.getHits() != null && objTransposeSearchResponse.getHits().getTotalHits() > 0
                        && objTransposeSearchResponse.getHits().getHits() != null
                        && objTransposeSearchResponse.getHits().getHits().length > 0);

                mapTransposeData.put(curHeaderID, mapParamValue);
            });

            //-Convert map transpose data to list
            lstAllParam.addAll(mapTransposeParam.keySet());
            lstAllHeader.addAll(lstAllParam);
            ConcurrentHashMap<String, List<Object>> mapConversion = new ConcurrentHashMap<>();

            mapMasterData.keySet().parallelStream().forEach(curHeaderID -> {
                String strMasterCSV = mapMasterData.get(curHeaderID);
                List<String> lstCurParam = new ArrayList<>();
                Map<String, Object> mapCurParam = mapTransposeData.get(curHeaderID);

                for (int intCountParam = 0; intCountParam < lstAllParam.size(); intCountParam++) {
                    if (mapCurParam.containsKey(lstAllParam.get(intCountParam))) {
                        lstCurParam.add(mapCurParam.get(lstAllParam.get(intCountParam)).toString());
                    } else {
                        lstCurParam.add("");
                    }
                }

                mapConversion.put(curHeaderID, Arrays.asList(strMasterCSV, Strings.join(lstCurParam, ",")));
            });

            //-Write CSV
            String strNewFile = FileUtil.createFile(strFileName);

            if (strNewFile != null && !strNewFile.isEmpty()) {
                FileWriter objFileWriter = new FileWriter(strNewFile, true);

                //Write Header
                CSVUtil.writeLine(objFileWriter, lstAllHeader, ',');
                //Write Data
                mapConversion.forEach((curKey, curData) -> {
                    try {
                        CSVUtil.writeLine(objFileWriter, curData, ',');
                    } catch (Exception objEx) {
                        objLogger.debug("WARN: " + ExceptionUtil.getStackTrace(objEx));
                    }
                });

                if (objFileWriter != null) {
                    objFileWriter.flush();
                    objFileWriter.close();

                    lstExportedFile.add(strNewFile);
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("WARN: " + ExceptionUtil.getStackTrace(objEx));
        }

        Long lElapsedTime = Calendar.getInstance().getTimeInMillis() - objBegin.getTimeInMillis();

        if (lstExportedFile != null && lstExportedFile.size() > 0) {
            for (int intCount = 0; intCount < lstExportedFile.size(); intCount++) {
                File objFile = new File(lstExportedFile.get(intCount));

                if (objFile.exists()) {
                    ESFileModel objCurFileModel = new ESFileModel();
                    objCurFileModel.setFile_name(objFile.getName());
                    objCurFileModel.setFile_path(objFile.getAbsolutePath());
                    objCurFileModel.setFile_size(objFile.length());
                    objCurFileModel.setProcessed_time(lElapsedTime);

                    lstReturnFile.add(objCurFileModel);
                }
            }
        }

        return lstReturnFile;
    }

    public String exportESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest) {
        return exportPatternESDataToCSV(strIndex, strType, strFileName, intPageSize, objFilterAllRequest, false, 0).get(0);
    }

    public String exportESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize) {
        return exportESDataToCSV(strIndex, strType, strFileName, intPageSize, null);
    }

    public List<ESFileModel> exportESDataToCSV(String strIndexPattern, String strType, String strFilePattern, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest, Boolean bIsMultipleFile, Integer inMaxFileLine) {
        return exportESDataToCSV(strIndexPattern, strType, null, strFilePattern, intPageSize, objFilterAllRequest, bIsMultipleFile, inMaxFileLine);
    }

    public List<ESFileModel> exportESDataToCSV(HashMap<String, String> mapTypeIndex, String strFilePattern, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest, Boolean bIsMultipleFile, Integer intMaxFileLine) {
        return exportESDataToCSV(null, null, mapTypeIndex, strFilePattern, intPageSize, objFilterAllRequest, bIsMultipleFile, intMaxFileLine);
    }

    public Boolean prepESData(List<ESPrepAbstractModel> lstPrepOp) {
        Boolean bIsPrepAll = false;

        try {
            HashMap<String, String> mapIndexMapping = new HashMap<>();

            if (objESClient != null && lstPrepOp != null && lstPrepOp.size() > 0) {
                for (int intCount = 0; intCount < lstPrepOp.size(); intCount++) {
                    ESPrepAbstractModel objPrepOp = lstPrepOp.get(intCount);
                    String strCurIndex = objESConnection.getLatestIndexName(mapIndexMapping, objPrepOp.getIndex());

                    bIsPrepAll = false;

                    if (objPrepOp instanceof ESPrepFieldModel) {
                        ESPrepFieldModel objPrep = (ESPrepFieldModel) objPrepOp;

                        if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                            bIsPrepAll = handleFields(strCurIndex, objPrep.getType(), objPrep);

                            if (!bIsPrepAll) {
                                break;
                            }
                        }
                    }

                    if (objPrepOp instanceof ESPrepDocModel) {
                        ESPrepDocModel objPrep = (ESPrepDocModel) objPrepOp;

                        if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                            HashMap<String, Integer> mapNumTimeCopyDoc = new HashMap<>();

                            if (objPrep.getCopy_doc_ids() != null && objPrep.getCopy_doc_nums() != null
                                    && objPrep.getCopy_doc_ids().size() == objPrep.getCopy_doc_nums().size()) {
                                for (int intCountCopy = 0; intCountCopy < objPrep.getCopy_doc_ids()
                                        .size(); intCountCopy++) {
                                    mapNumTimeCopyDoc.put(objPrep.getCopy_doc_ids().get(intCountCopy),
                                            objPrep.getCopy_doc_nums().get(intCountCopy));
                                }
                            }

                            bIsPrepAll = handleDocuments(strCurIndex, objPrep.getType(), objPrep.getRemove_doc_ids(),
                                    mapNumTimeCopyDoc);

                            if (!bIsPrepAll) {
                                break;
                            }
                        }
                    }

                    if (objPrepOp instanceof ESPrepFunctionSamplingModel) {
                        bIsPrepAll = handleSamplingAction((ESPrepFunctionSamplingModel) objPrepOp);

                        if (!bIsPrepAll) {
                            break;
                        }
                    }

                    if ((objPrepOp instanceof ESPrepFormatModel)
                            || (objPrepOp instanceof ESPrepDataTypeChangeModel)
                            || (objPrepOp instanceof ESPrepFunctionArithmeticModel)
                            || (objPrepOp instanceof ESPrepFunctionStatisticModel)) {
                        bIsPrepAll = prepBulkAction(strCurIndex, objPrepOp.getType(), objPrepOp, intNumBulkOperation);

                        if (!bIsPrepAll) {
                            break;
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsPrepAll;
    }

    public Boolean deleteField(String strIndex, String strType, String strField) {
        String strRemoveScript = "ctx._source.remove(\"" + strField + "\")";

        SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                .setSize(intNumBulkOperation).get();
        do {
            if (objSearchResponse != null && objSearchResponse.getHits() != null
                    && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0) {

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intNumBulkOperation);

                for (SearchHit objHit : objSearchResponse.getHits().getHits()) {
                    UpdateRequest objUpdateRequest = new UpdateRequest(strIndex, strType, objHit.getId());
                    objUpdateRequest.script(new Script(strRemoveScript));

                    objBulkProcessor.add(objUpdateRequest);
                }

                objBulkProcessor.flush();
                try {
                    objBulkProcessor.awaitClose(10l, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    objLogger.debug("Cannot delete field ({}) of index ({}). Error: {}", strField, strIndex, e.getMessage());
                    return false;
                }

                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                        .setScroll(new TimeValue(lScrollTTL)).get();
            } else {
                break;
            }
        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                && objSearchResponse.getHits().getHits() != null
                && objSearchResponse.getHits().getHits().length > 0);

        return true;
    }

    public Boolean deleteBulkData(String strIndex, String strType,
                                  ESFilterAllRequestModel objFilterAllRequest, Integer intPageSize) {
        Boolean bIsUpdated = false;

        try {
            if (objESClient != null) {
                //Refresh index before update
                objESConnection.refreshIndex(strIndex);

                List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);

                List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                        && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                        ? objFilterAllRequest.getFilters()
                        : new ArrayList<ESFilterRequestModel>();

                Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

                if (lstFilters != null && lstFilters.size() > 0) {
                    List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                    BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                    List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                    if (objQueryBuilder != null) {
                        if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                            objQueryBuilder = objESFilter.generateAggQueryBuilder(strIndex, strType, objQueryBuilder,
                                    lstNotAddedFilterRequest, lstFieldModel);
                        }

                        objSearchSourceBuilder.query(objQueryBuilder);
                    }
                }

                SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                        .setSource(objSearchSourceBuilder)
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                        .setSize(intPageSize).get();

                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {
                        BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intPageSize);

                        List<SearchHit> lstData = new ArrayList<SearchHit>();
                        lstData = Arrays.asList(objSearchResponse.getHits().getHits());

                        for (int intCount = 0; intCount < lstData.size(); intCount++) {
                            String strHitID = lstData.get(intCount).getId();
                            String strCurIndex = lstData.get(intCount).getIndex();
                            String strCurType = lstData.get(intCount).getType();

                            objBulkProcessor.add(new DeleteRequest(strCurIndex, strCurType, strHitID));
                        }

                        objBulkProcessor.flush();
                        objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                        objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                .setScroll(new TimeValue(lScrollTTL)).get();
                    } else {
                        break;
                    }
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);

                bIsUpdated = true;
            }
        } catch (Exception objEx) {
            bIsUpdated = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean updateBulkMapDataWithID(String strIndex, String strType, HashMap<String, HashMap<String, Object>> mapIDWithUpdateField) {
        Boolean bIsUpdated = false;

        try {
            if (objESClient != null && mapIDWithUpdateField != null && mapIDWithUpdateField.size() > 0) {
                //Refresh index before update
                objESConnection.refreshIndex(strIndex);

                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, 20000);

                for (Map.Entry<String, HashMap<String, Object>> curUpdatedID : mapIDWithUpdateField.entrySet()) {
                    String strUpdatedID = curUpdatedID.getKey();
                    Map<String, Object> mapUpdateField = curUpdatedID.getValue();

                    objBulkProcessor.add(new UpdateRequest(strIndex, strType, strUpdatedID).docAsUpsert(true)
                            .doc(objCurrentMapper.writeValueAsString(mapUpdateField), XContentType.JSON));
                }

                objBulkProcessor.flush();
                objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                bIsUpdated = true;
            }
        } catch (Exception objEx) {
            bIsUpdated = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean updateBulkDataWithID(String strIndex, String strType, HashMap<String, Object> mapIDWithUpdateField) {
        Boolean bIsUpdated = false;

        try {
            if (objESClient != null && mapIDWithUpdateField != null && mapIDWithUpdateField.size() > 0) {
                //Refresh index before update
                objESConnection.refreshIndex(strIndex);

                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, 20000);

                for (Map.Entry<String, Object> curUpdatedID : mapIDWithUpdateField.entrySet()) {
                    String strUpdatedID = curUpdatedID.getKey();
                    Object curUpdatedObj = curUpdatedID.getValue();

                    objBulkProcessor.add(new UpdateRequest(strIndex, strType, strUpdatedID).docAsUpsert(true)
                            .doc(objCurrentMapper.writeValueAsString(curUpdatedObj), XContentType.JSON));
                }

                objBulkProcessor.flush();
                objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                bIsUpdated = true;
            }
        } catch (Exception objEx) {
            bIsUpdated = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean updateBulkData(String strIndex, String strType,
                                  ESFilterAllRequestModel objFilterAllRequest, HashMap<String, Object> mapUpdateFieldValue,
                                  Integer intPageSize) {
        Boolean bIsUpdated = false;

        try {
            if (objESClient != null && mapUpdateFieldValue != null && mapUpdateFieldValue.size() > 0) {
                //Refresh index before update
                objESConnection.refreshIndex(strIndex);

                List<ESFieldModel> lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);

                List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                        && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                        ? objFilterAllRequest.getFilters()
                        : new ArrayList<ESFilterRequestModel>();

                Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();

                if (lstFilters != null && lstFilters.size() > 0) {
                    List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilters, lstFieldModel, new ArrayList<>(), bIsReversedFilter);
                    BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                    List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                    if (objQueryBuilder != null) {
                        if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                            objQueryBuilder = objESFilter.generateAggQueryBuilder(strIndex, strType, objQueryBuilder,
                                    lstNotAddedFilterRequest, lstFieldModel);
                        }

                        objSearchSourceBuilder.query(objQueryBuilder);
                    }
                }

                SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                        .setSource(objSearchSourceBuilder)
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lScrollTTL))
                        .setSize(intPageSize).get();

                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {
                        BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, intPageSize);

                        List<SearchHit> lstData = new ArrayList<SearchHit>();
                        lstData = Arrays.asList(objSearchResponse.getHits().getHits());

                        for (int intCount = 0; intCount < lstData.size(); intCount++) {
                            Map<String, Object> mapCurHit = lstData.get(intCount).getSourceAsMap();
                            String strHitID = lstData.get(intCount).getId();

                            for (Map.Entry<String, Object> curUpdateItem : mapUpdateFieldValue.entrySet()) {
                                mapCurHit.put(curUpdateItem.getKey(), curUpdateItem.getValue());
                            }

                            objBulkProcessor.add(new UpdateRequest(lstData.get(intCount).getIndex(), lstData.get(intCount).getType(), strHitID)
                                    .docAsUpsert(true)
                                    .doc(objCurrentMapper.writeValueAsString(mapUpdateFieldValue), XContentType.JSON));
                        }

                        objBulkProcessor.flush();
                        objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                        objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                .setScroll(new TimeValue(lScrollTTL)).get();
                    } else {
                        break;
                    }
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);

                bIsUpdated = true;
            }
        } catch (Exception objEx) {
            bIsUpdated = false;
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean updateBulkData(String strIndex, String strType, List<?> lstData, String strIDField) {
        Boolean bIsUpdated = false;

        try {
            if (objESConnection != null && lstData != null && lstData.size() > 0 && strIDField != null && !strIDField.isEmpty()) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    List<String> lstID = new ArrayList<>();
                    List<HashMap<String, Object>> lstDataWithoutID = new ArrayList<>();
                    HashMap<String, Object> mapOriginal = new HashMap<>();

                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        if (objData instanceof HashMap) {
                            mapOriginal = (HashMap<String, Object>) objData;
                        } else {
                            mapOriginal = objCurrentMapper.convertValue(objData, HashMap.class);
                        }

                        if (mapOriginal.containsKey(strIDField)) {
                            lstID.add(mapOriginal.get(strIDField).toString());
                            mapOriginal.remove(strIDField);

                            lstDataWithoutID.add(mapOriginal);
                        }
                    }

                    for (int intCount = 0; intCount < lstDataWithoutID.size(); intCount++) {
                        HashMap<String, Object> mapData = lstDataWithoutID.get(intCount);

                        objBulkProcessor.add(new UpdateRequest(strIndex, strType, lstID.get(intCount))
                                .docAsUpsert(true)
                                .doc(objCurrentMapper.writeValueAsString(mapData), XContentType.JSON));
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsUpdated = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strIDField, String strFieldDate, List<ESFieldModel> lstFieldModel) {
        Boolean bIsInserted = false;

        try {
            if (objESClient != null && strIDField != null && !strIDField.isEmpty()) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    List<String> lstID = new ArrayList<>();
                    List<HashMap<String, Object>> lstDataWithoutID = new ArrayList<>();
                    HashMap<String, Object> mapOriginal = new HashMap<>();
                    Map<String, Map<String, String>> mapDataType = new HashMap<>();

                    if (lstFieldModel == null || lstFieldModel.size() <= 0) {
                        Object objData = lstData.get(0);

                        if (objData instanceof HashMap) {
                            mapOriginal = (HashMap<String, Object>) objData;
                        } else {
                            ObjectMapper objObjectMapper = new ObjectMapper();
                            mapOriginal = objObjectMapper.convertValue(objData, HashMap.class);
                            Class<?> classZ = lstData.get(0).getClass();

                            for (Map.Entry<String, Object> curField : mapOriginal.entrySet()) {
                                try {
                                    String strFieldType = classZ.getDeclaredField(curField.getKey()).getType().getTypeName()
                                            .toLowerCase();

                                    Map<String, String> mapFieldFormat = new HashMap<>();
                                    mapFieldFormat.put(ESPithosConstant.PREDEFINED_DATA_TYPE, strFieldType);

                                    mapDataType.put(curField.getKey(), mapFieldFormat);
                                } catch (Exception objEx) {
                                }
                            }
                        }

                        if (mapOriginal.containsKey(strIDField)) {
                            lstID.add(mapOriginal.get(strIDField).toString());
                            mapOriginal.remove(strIDField);

                            lstDataWithoutID.add(mapOriginal);
                        }

                        objESConnection.createIndex(strIndex, strType, lstDataWithoutID, strFieldDate, null, false, mapDataType);
                        lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);
                    }

                    lstID = new ArrayList<>();
                    lstDataWithoutID = new ArrayList<>();

                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        if (objData instanceof HashMap) {
                            mapOriginal = (HashMap<String, Object>) objData;
                            mapOriginal = ESConverterUtil.convertMapToMapType(mapOriginal, lstFieldModel);
                        } else {
                            mapOriginal = objCurrentMapper.convertValue(objData, HashMap.class);
                        }

                        if (mapOriginal.containsKey(strIDField)) {
                            lstID.add(mapOriginal.get(strIDField).toString());
                            mapOriginal.remove(strIDField);

                            lstDataWithoutID.add(mapOriginal);
                        }
                    }

                    for (int intCount = 0; intCount < lstDataWithoutID.size(); intCount++) {
                        HashMap<String, Object> mapData = lstDataWithoutID.get(intCount);

                        objBulkProcessor.add(new IndexRequest(strIndex, strType).id(lstID.get(intCount))
                                .source(objCurrentMapper.writeValueAsString(mapData), XContentType.JSON));
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsInserted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsInserted;
    }

    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel, Boolean bIsUsedAutoID, String strDocIdPrefix) {
        return insertBulkData(strIndex, strType, lstData, strFieldDate, lstFieldModel, bIsUsedAutoID, strDocIdPrefix, null);
    }

    public Boolean insertBulkHashData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel,
                                      Boolean bIsUsedAutoID, String strDocIdPrefix, Map<String, Map<String, String>> mapPredefinedDataType) {
        Boolean bIsInserted = false;

        try {
            if (lstFieldModel == null || lstFieldModel.size() <= 0) {
                objESConnection.createIndex(strIndex, strType, lstData, strFieldDate, null, false, mapPredefinedDataType);
                lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);
            }

            if (objESClient != null) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        String strGenerateId = strDocIdPrefix + "_" + strIndex + "_" + strType + "_" + intCount;

                        HashMap<String, Object> mapOriginal = (HashMap<String, Object>) objData;

                        mapOriginal = ESConverterUtil.convertMapToMapType(mapOriginal, lstFieldModel);

                        if (bIsUsedAutoID) {
                            objBulkProcessor.add(new IndexRequest(strIndex, strType)
                                    .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                        } else {
                            objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strGenerateId)
                                    .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                        }
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsInserted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsInserted;
    }

    public Boolean insertBulkHashData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel,
                                      Boolean bIsUsedAutoID, String strDocIdPrefix, HashMap<String, String> mapPredefinedDataType) {
        Boolean bIsInserted = false;

        try {
            if (lstFieldModel == null || lstFieldModel.size() <= 0) {
                Map<String, Map<String, String>> mapConvertFieldDataType = null;

                if (mapPredefinedDataType != null) {
                    mapConvertFieldDataType = new HashMap<>();

                    for (Map.Entry<String, String> curFieldType : mapPredefinedDataType.entrySet()) {
                        Map<String, String> mapFieldFormat = new HashMap<>();
                        mapFieldFormat.put(ESPithosConstant.PREDEFINED_DATA_TYPE, curFieldType.getValue());

                        mapConvertFieldDataType.put(curFieldType.getKey(), mapFieldFormat);
                    }
                }

                objESConnection.createIndex(strIndex, strType, lstData, strFieldDate, null, false, mapConvertFieldDataType);
                lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);
            }

            if (objESClient != null) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        String strGenerateId = strDocIdPrefix + "_" + strIndex + "_" + strType + "_" + intCount;

                        HashMap<String, Object> mapOriginal = (HashMap<String, Object>) objData;

                        mapOriginal = ESConverterUtil.convertMapToMapType(mapOriginal, lstFieldModel);

                        if (bIsUsedAutoID) {
                            objBulkProcessor.add(new IndexRequest(strIndex, strType)
                                    .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                        } else {
                            objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strGenerateId)
                                    .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                        }
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsInserted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsInserted;
    }

    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel, Boolean bIsUsedAutoID, String strDocIdPrefix, Map<String, Map<String, String>> mapPredefinedDataType) {
        Boolean bIsInserted = false;

        try {
            if (lstFieldModel == null || lstFieldModel.size() <= 0) {
                objESConnection.createIndex(strIndex, strType, lstData, strFieldDate, null, false, mapPredefinedDataType);
                lstFieldModel = objESConnection.getFieldsMetaData(strIndex, strType, null, false);
            }

            if (objESClient != null) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        String strGenerateId = strDocIdPrefix + "_" + strIndex + "_" + strType + "_" + intCount;

                        if (objData instanceof HashMap) {
                            HashMap<String, Object> mapOriginal = (HashMap<String, Object>) objData;

                            mapOriginal = ESConverterUtil.convertMapToMapType(mapOriginal, lstFieldModel);

                            if (bIsUsedAutoID) {
                                objBulkProcessor.add(new IndexRequest(strIndex, strType)
                                        .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                            } else {
                                objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strGenerateId)
                                        .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
                            }
                        } else {
                            if (bIsUsedAutoID) {
                                objBulkProcessor.add(new IndexRequest(strIndex, strType)
                                        .source(objCurrentMapper.writeValueAsString(lstData.get(intCount)), XContentType.JSON));
                            } else {
                                objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strGenerateId)
                                        .source(objCurrentMapper.writeValueAsString(lstData.get(intCount)), XContentType.JSON));
                            }
                        }
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsInserted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.debug("ERR: " + ExceptionUtil.getStackTrace(objEx));
        }

        return bIsInserted;
    }

    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel) {
        return insertBulkData(strIndex, strType, lstData, strFieldDate, lstFieldModel, false, "");
    }
}
