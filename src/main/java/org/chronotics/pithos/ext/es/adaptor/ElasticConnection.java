package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chronotics.pandora.java.converter.ConverterUtil;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.chronotics.pandora.java.serialization.JacksonFilter;
import org.chronotics.pithos.ext.es.model.*;
import org.chronotics.pithos.ext.es.util.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class ElasticConnection {
    Logger objLogger = LoggerFactory.getLogger(ElasticConnection.class);
    String strESClusterName = "";
    String strESCoorNodeIP = "";
    String strTransportUsername = "";
    String strTransportPassword = "";
    Integer intESCoorNodePort = 0;
    Integer intNumBulkOperation = 20000;
    TransportClient objESClient;
    List<String> lstConvertedDataType = new ArrayList<>();

    Integer intNumReplica = 0;
    Boolean bIsUseHotWarm = false;

    public static ElasticConnection instance;

    ObjectMapper objMapper = new ObjectMapper();

    public ElasticConnection(String strESClusterName, String strESCoorNodeIP, Integer intESCoorNodePort, String strTransportUsername, String strTransportPassword) {
        this.strESClusterName = strESClusterName;
        this.strESCoorNodeIP = strESCoorNodeIP;
        this.intESCoorNodePort = intESCoorNodePort;
        this.strTransportUsername = strTransportUsername;
        this.strTransportPassword = strTransportPassword;

        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_BYTE);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_DATE);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_DOUBLE);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_BOOLEAN);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_FLOAT);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_INTEGER);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_LONG);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_NUMERIC);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_SHORT);
        this.lstConvertedDataType.add(ESFilterOperationConstant.DATA_TYPE_TEXT);

        objESClient = createESClient();
    }

    public static ElasticConnection getInstance(String strESClusterName, String strESCoorNodeIP,
                                                Integer intESCoorNodePort) {
        if (instance == null) {
            synchronized (ElasticConnection.class) {
                if (instance == null) {
                    instance = new ElasticConnection(strESClusterName, strESCoorNodeIP, intESCoorNodePort, "", "");
                }
            }
        }

        return instance;
    }

    public static ElasticConnection getInstance(String strESClusterName, String strESCoorNodeIP,
                                                Integer intESCoorNodePort, String strTransportUsername, String strTransportPassword) {
        if (instance == null) {
            synchronized (ElasticConnection.class) {
                if (instance == null) {
                    instance = new ElasticConnection(strESClusterName, strESCoorNodeIP, intESCoorNodePort, strTransportUsername, strTransportPassword);
                }
            }
        }

        return instance;
    }

    public void setNumReplica(Integer intNumReplica) {
        this.intNumReplica = intNumReplica;
    }

    public void setIsUseHotWarm(Boolean bIsUseHotWarm) {
        this.bIsUseHotWarm = bIsUseHotWarm;
    }

    @SuppressWarnings("resource")
    protected TransportClient createESClient() {
        TransportClient objESClient = null;

        try {
            if (this.strTransportUsername == null || this.strTransportUsername.isEmpty()) {
                Settings objSetting = Settings.builder().put("cluster.name", strESClusterName)
                        .put("client.transport.sniff", false).build();
                objESClient = new PreBuiltTransportClient(objSetting, MatrixAggregationPlugin.class).addTransportAddress(
                        new TransportAddress(InetAddress.getByName(strESCoorNodeIP), intESCoorNodePort));
            } else {
                Settings objSetting = Settings.builder().put("cluster.name", strESClusterName)
                        .put("client.transport.sniff", false)
                        .put("xpack.security.user", this.strTransportUsername + ":" + this.strTransportPassword)
                        .build();
                objESClient = new PreBuiltXPackTransportClient(objSetting, MatrixAggregationPlugin.class)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName(strESCoorNodeIP), intESCoorNodePort));
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objESClient;
    }

    public void closeInstance() {
        try {
            if (objESClient != null) {
                objESClient.close();
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }
    }

    protected List<Object> createESAdminClient() {
        List<Object> lstClient = new ArrayList<>();

        AdminClient objClient = null;
        try {
            objClient = objESClient.admin();

            lstClient.add(objESClient);
            lstClient.add(objClient);
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstClient;
    }

    protected IndicesAdminClient createESIndiceAdminClient() {
        IndicesAdminClient objClient = null;

        try {
            objClient = objESClient.admin().indices();
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objClient;
    }



    @SuppressWarnings("unchecked")
    protected Map<String, Map<String, List<ESFieldModel>>> getFieldsOfIndices(List<String> lstIndex, List<String> lstType,
                                                                            List<String> lstField, Boolean bIsCheckNull) {
        Map<String, Map<String, List<ESFieldModel>>> mapFields = new HashMap<>();

        try {
            String[] arrField = {"*"};

            if (lstField != null && lstField.size() > 0) {
                arrField = lstField.toArray(new String[lstField.size()]);
            }

            IndicesAdminClient objAdminClient = createESIndiceAdminClient();
            GetFieldMappingsResponse objFieldMappingResponse = objAdminClient
                    .prepareGetFieldMappings(lstIndex.toArray(new String[lstIndex.size()]))
                    .setTypes(lstType.toArray(new String[lstType.size()])).setFields(arrField).get();

            if (objFieldMappingResponse != null && objFieldMappingResponse.mappings() != null
                    && objFieldMappingResponse.mappings().size() > 0) {
                for (Map.Entry<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> curIndex : objFieldMappingResponse
                        .mappings().entrySet()) {
                    String strCurIndex = curIndex.getKey();
                    Map<String, List<ESFieldModel>> mapType = new HashMap<>();

                    for (Map.Entry<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> curType : curIndex
                            .getValue().entrySet()) {
                        String strCurType = curType.getKey();
                        List<ESFieldModel> lstESField = new ArrayList<>();
                        lstField = new ArrayList<>();

                        for (Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetaData> curField : curType
                                .getValue().entrySet()) {
                            if (!curField.getKey().contains(".keyword") && !curField.getKey().equals("_index")
                                    && !curField.getKey().equals("_all") && !curField.getKey().equals("_parent")
                                    && !curField.getKey().equals("_version") && !curField.getKey().equals("_routing")
                                    && !curField.getKey().equals("_type") && !curField.getKey().equals("_seq_no")
                                    && !curField.getKey().equals("_field_names") && !curField.getKey().equals("_source")
                                    && !curField.getKey().equals("_id") && !curField.getKey().equals("_uid")
                                    && !curField.getKey().equals("_ignored")) {
                                ESFieldModel objFieldModel = new ESFieldModel();
                                objFieldModel.setFull_name(curField.getValue().fullName());
                                lstField.add(curField.getValue().fullName());

                                Map<String, Object> mapProperty = curField.getValue().sourceAsMap();

                                if (mapProperty != null && mapProperty.size() > 0
                                        && mapProperty.get(curField.getValue().fullName()) instanceof HashMap) {
                                    HashMap<String, Object> mapCurType = ((HashMap<String, Object>) mapProperty
                                            .get(curField.getValue().fullName()));

                                    if (mapCurType != null && mapCurType.containsKey("type")) {
                                        String strFieldDataType = mapCurType.get("type").toString();

                                        if (strFieldDataType.equals("text")) {
                                            if (curType.getValue()
                                                    .containsKey(curField.getValue().fullName() + ".keyword")) {
                                                objFieldModel.setFielddata(true);
                                            } else {
                                                objFieldModel.setFielddata(false);
                                            }
                                        }

                                        objFieldModel.setType(mapCurType.get("type").toString());
                                    }
                                }

                                if (objFieldModel.getType() != null && !objFieldModel.getType().equals("text")) {
                                    lstESField.add(objFieldModel);
                                }
                            }
                        }

                        if (bIsCheckNull) {
                            // Make sure the list of Fields doesnt contain any empty field
                            List<String> notNullField = getNotNullField(strCurIndex, strCurType, lstField);
                            List<ESFieldModel> lstNotNullESField = new ArrayList<>();
                            for (ESFieldModel fd : lstESField) {
                                if (!bIsCheckNull || notNullField.contains(fd.getFull_name())) {
                                    lstNotNullESField.add(fd);
                                }
                            }
                            mapType.put(strCurType, lstNotNullESField);
                        } else  {
                            mapType.put(strCurType, lstESField);
                        }
                    }

                    mapFields.put(strCurIndex, mapType);

                    break;
                }
            }

        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapFields;
    }

    protected void closeESClient(TransportClient objESClient) {
        // try {
        // if (objESClient != null) {
        // objESClient.close();
        // objESClient.threadPool().shutdown();
        // objESClient = null;
        // }
        // } catch (Exception objEx) {
        // objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        // }
    }

    protected String generateMergingIDScript(MergingDataRequestModel objMergingRequestModel) {
        StringBuilder objMergingStr = new StringBuilder();

        if (objMergingRequestModel != null && objMergingRequestModel.getIndex_fields() != null
                && objMergingRequestModel.getIndex_fields().size() > 0) {
            Map<String, List<String>> lstUniqueKeyOfIndex = new HashMap<>();

            String strCurIndexName = "";
            List<String> lstDefinedField = new ArrayList<>();

            if (objMergingRequestModel.getUnique_index_name() != null
                    && !objMergingRequestModel.getUnique_index_name().isEmpty()) {
                strCurIndexName = objMergingRequestModel.getUnique_index_name();
                lstDefinedField = objMergingRequestModel.getUnique_field();

                if (lstDefinedField == null || lstDefinedField.size() <= 0) {
                    lstDefinedField = new ArrayList<>();

                    for (int intCount = 0; intCount < objMergingRequestModel.getIndex_fields().size(); intCount++) {
                        if (objMergingRequestModel.getIndex_fields().get(intCount).getIndex_name()
                                .equals(strCurIndexName)) {
                            lstDefinedField
                                    .add(objMergingRequestModel.getIndex_fields().get(intCount).getIndex_field());
                            break;
                        }
                    }
                }
            } else {
                strCurIndexName = objMergingRequestModel.getIndex_fields().get(0).getIndex_name();
                lstDefinedField = new ArrayList<>();
                lstDefinedField.add(objMergingRequestModel.getIndex_fields().get(0).getIndex_field());

                for (int intCount = 1; intCount < objMergingRequestModel.getIndex_fields().size(); intCount++) {
                    lstDefinedField.add(objMergingRequestModel.getIndex_fields().get(intCount).getIndex_field());
                }
            }

            if (!strCurIndexName.isEmpty() && lstDefinedField.size() > 0) {
                lstUniqueKeyOfIndex.put(strCurIndexName, lstDefinedField);

                HashMap<String, List<String>> mapRelatedIndex = new HashMap<>();

                for (int intCountField = 0; intCountField < lstDefinedField.size(); intCountField++) {
                    for (int intCount = 0; intCount < objMergingRequestModel.getIndex_fields().size(); intCount++) {
                        MergingDataIndexModel objDataIndex = objMergingRequestModel.getIndex_fields().get(intCount);

                        if (objDataIndex.getIndex_name().equals(strCurIndexName)
                                && objDataIndex.getIndex_field().equals(lstDefinedField.get(intCountField))) {
                            for (int intCountRelated = 0; intCountRelated < objDataIndex.getRelated_index_name()
                                    .size(); intCountRelated++) {
                                String strRelatedIndex = objDataIndex.getRelated_index_name().get(intCountRelated);
                                String strRelatedField = objDataIndex.getRelated_index_field().get(intCountRelated);

                                if (mapRelatedIndex.containsKey(strRelatedIndex)) {
                                    mapRelatedIndex.get(strRelatedIndex).add(strRelatedField);
                                } else {
                                    List<String> lstRelatedField = new ArrayList<>();
                                    lstRelatedField.add(strRelatedField);

                                    mapRelatedIndex.put(strRelatedIndex, lstRelatedField);
                                }
                            }

                            break;
                        }
                    }
                }

                if (mapRelatedIndex != null && mapRelatedIndex.size() > 0) {
                    for (Map.Entry<String, List<String>> curEntry : mapRelatedIndex.entrySet()) {
                        lstUniqueKeyOfIndex.put(curEntry.getKey(), curEntry.getValue());
                    }
                }
            }

            if (lstUniqueKeyOfIndex != null && lstUniqueKeyOfIndex.size() > 0) {
                int intCountIndex = 0;

                for (Map.Entry<String, List<String>> curEntry : lstUniqueKeyOfIndex.entrySet()) {
                    StringBuilder objCurScriptBuilder = new StringBuilder();

                    String strCurIndex = curEntry.getKey();
                    List<String> lstCurField = curEntry.getValue();

                    if (intCountIndex > 0) {
                        objCurScriptBuilder.append(" else ");
                    }

                    objCurScriptBuilder.append("if (ctx._index == \\\"").append(strCurIndex).append("\\\") {")
                            .append(" ctx._id = ");

                    for (int intCountCurField = 0; intCountCurField < lstCurField.size(); intCountCurField++) {
                        if (intCountCurField > 0) {
                            objCurScriptBuilder.append("+");
                        }
                        objCurScriptBuilder.append("ctx._source").append(ConverterUtil.convertDashField(lstCurField.get(intCountCurField)));
                    }

                    objCurScriptBuilder.append(" }");

                    objMergingStr.append(objCurScriptBuilder);

                    intCountIndex++;
                }
            }
        }

        return objMergingStr.toString();
    }

    public ESMatrixStatModel statsMatrix(String strIndex, String strType, ESFilterAllRequestModel objFilterAllRequestModel) {
        List<String> lstFields = objFilterAllRequestModel.getSelected_fields();
        ESMatrixStatModel objMatrixStat = new ESMatrixStatModel();
        String strStatName = "statistic";

        try {
            List<String> lstStatFields = new ArrayList<>();

            if (lstFields == null || lstFields.size() <= 1) {
                Map<String, Map<String, List<ESFieldModel>>> mapFoundFields = getFieldsOfIndices(Arrays.asList(strIndex), Arrays.asList(strType), new ArrayList<>(), true);

                if (mapFoundFields.containsKey(strIndex) && mapFoundFields.get(strIndex).containsKey(strType)) {
                    lstStatFields = mapFoundFields.get(strIndex).get(strType).stream()
                            .filter(objESField -> !objESField.getType().equals(ESFilterOperationConstant.DATA_TYPE_DATE)
                                    && !objESField.getType().equals(ESFilterOperationConstant.DATA_TYPE_TEXT)
                                    && !objESField.getType().equals(ESFilterOperationConstant.DATA_TYPE_BOOLEAN))
                            .map(objESField -> objESField.getFull_name()).collect(Collectors.toList());
                }

                if (lstFields == null || lstFields.size() <= 0) {
                    lstFields = new ArrayList<>();
                    lstFields = lstStatFields;
                }
            } else {
                lstStatFields = lstFields;
            }

            Integer intFilterType = 0;
            Double dbCustomValue = null;

            for (ESFilterRequestModel objESFilterRequestModel : objFilterAllRequestModel.getFilters()) {
                intFilterType = objESFilterRequestModel.getFiltered_operation();

                if (lstFields != null && lstFields.size() == 1) {
                    dbCustomValue = Double.valueOf(objESFilterRequestModel.getFiltered_conditions().get(0));
                }

                break;
            }

            if (objESClient != null && lstStatFields != null && lstStatFields.size() > 0) {
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.size(0);
                objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                MatrixStatsAggregationBuilder objMatrixStatBuilder = MatrixStatsAggregationBuilders.matrixStats(strStatName).fields(lstStatFields);
                objSearchRequestBuilder.addAggregation(objMatrixStatBuilder);

                BoolQueryBuilder objBooleanQueryBuilder = new BoolQueryBuilder();

                for (int intCount = 0; intCount < lstStatFields.size(); intCount++) {
                    ExistsQueryBuilder objExistQueryBuilder = new ExistsQueryBuilder(lstStatFields.get(intCount));
                    objBooleanQueryBuilder.must(objExistQueryBuilder);
                }

                objSearchRequestBuilder.setQuery(objBooleanQueryBuilder);

                SearchResponse objSearchResponse = objSearchRequestBuilder.get();

                if (objSearchResponse != null && objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getAggregations() != null) {
                    if (objSearchResponse.getAggregations().get(strStatName) != null) {
                        MatrixStats objStat = (MatrixStats) objSearchResponse.getAggregations().get(strStatName);

                        if (objStat != null) {
                            objMatrixStat.setField_stats(new ArrayList<>());

                            if (lstFields.size() > 1) {
                                objMatrixStat.setFields(new ArrayList<>(lstFields));
                            } else {
                                objMatrixStat.setFields(new ArrayList<>());
                            }

                            for (int intCount = 0; intCount < lstFields.size(); intCount++) {
                                String strCurField = lstFields.get(intCount);
                                ESMatrixFieldStatModel objFieldStatModel = new ESMatrixFieldStatModel();
                                objFieldStatModel.setField(strCurField);
                                objFieldStatModel.setCount(objStat.getDocCount());
                                objFieldStatModel.setMean(objStat.getMean(strCurField));
                                objFieldStatModel.setKurtosis(objStat.getKurtosis(strCurField));
                                objFieldStatModel.setVariance(objStat.getVariance(strCurField));
                                objFieldStatModel.setSkewness(objStat.getSkewness(strCurField));

                                List<Double> lstCorr = new ArrayList<>();
                                List<Double> lstCov = new ArrayList<>();

                                for (int intCountField = 0; intCountField < lstStatFields.size(); intCountField++) {
                                    String strCurFieldCorr = lstStatFields.get(intCountField);

                                    Boolean bCanAdd = false;

                                    switch (intFilterType) {
                                        case ESFilterOperationConstant.CORRELATION:
                                            if (dbCustomValue == null || (objStat.getCorrelation(strCurField, strCurFieldCorr) >= dbCustomValue)) {
                                                lstCorr.add(objStat.getCorrelation(strCurField, strCurFieldCorr));
                                                lstCov.add(objStat.getCovariance(strCurField, strCurFieldCorr));

                                                bCanAdd = true;
                                            }

                                            break;
                                        case ESFilterOperationConstant.COVARIANCE:
                                            if (dbCustomValue == null || (objStat.getCovariance(strCurField, strCurFieldCorr) >= dbCustomValue)) {
                                                lstCorr.add(objStat.getCorrelation(strCurField, strCurFieldCorr));
                                                lstCov.add(objStat.getCovariance(strCurField, strCurFieldCorr));

                                                bCanAdd = true;
                                            }
                                            break;
                                    }

                                    if (bCanAdd) {
                                        if (lstFields.size() == 1 && !objMatrixStat.getFields().contains(strCurFieldCorr)) {
                                            objMatrixStat.getFields().add(strCurFieldCorr);
                                        }
                                    }
                                }

                                objFieldStatModel.setCorrelations(lstCorr);
                                objFieldStatModel.setCovariances(lstCov);

                                objMatrixStat.getField_stats().add(objFieldStatModel);
                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objMatrixStat;
    }

    protected Map<String, Map<String, ESMappingFieldModel>> createNewMappingField(String strConvertedDataType, String strNewField) {
        Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties = new HashMap<>();
        Map<String, ESMappingFieldModel> mapFieldMapping = new HashMap<>();

        ESMappingFieldModel objMappingField = createMappingField(strConvertedDataType,
                strConvertedDataType.equals(ESFilterOperationConstant.DATA_TYPE_DATE) ? true : false);
        mapFieldMapping.put(strNewField, objMappingField);
        mapFieldProperties.put("properties", mapFieldMapping);

        return mapFieldProperties;
    }

    protected List<String> getNotNullField(String strIndex, String strType, List<String> lstField) {
        List<String> lstNotNullField = new ArrayList<>();

        try {
            if (objESClient != null) {
                Long lTotalHit = 0l;

                //Get Total Hit First
                SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);

                MatchAllQueryBuilder objMatchAllQuery = new MatchAllQueryBuilder();
                SearchResponse objSearchResponse = objRequestBuilder.setQuery(objMatchAllQuery).get();

                if (objSearchResponse != null && objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() >= 0) {
                    lTotalHit = objSearchResponse.getHits().getTotalHits();
                }

                objRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                SearchSourceBuilder objSourceBuilder = new SearchSourceBuilder();
                objSourceBuilder.size(0);
                objRequestBuilder.setSource(objSourceBuilder);

                for (String strField : lstField) {
                    objRequestBuilder.addAggregation(AggregationBuilders.filter(strField + "_null", QueryBuilders.existsQuery(strField)));
                }

                SearchResponse objNullResponse = objRequestBuilder.get();

                if (objNullResponse != null && objNullResponse.getHits() != null
                        && objNullResponse.getHits().getTotalHits() > 0
                        && objNullResponse.getAggregations() != null) {
                    List<Aggregation> lstNullAggs = objNullResponse.getAggregations().asList();

                    for (int intCount = 0; intCount < lstNullAggs.size(); intCount++) {
                        String strCurFieldName = lstNullAggs.get(intCount).getName().replace("_null", "");

                        if (lstNullAggs.get(intCount).getName().contains("_null")) {
                            //Long lTotalDoc = ((InternalValueCount) lstNullAggs.get(intCount)).getValue();
                            Long lTotalDoc = ((InternalFilter) lstNullAggs.get(intCount)).getDocCount();

                            if (lTotalDoc.doubleValue() / lTotalHit.doubleValue() < 1.1
                                    && lTotalDoc.doubleValue() / lTotalHit.doubleValue() > 0.1) {
                                lstNotNullField.add(strCurFieldName);
                            }

//                            if (lTotalHit.doubleValue() / lTotalDoc.doubleValue() < 1.1
//                                    && lTotalHit.doubleValue() / lTotalDoc.doubleValue() > 0.9) {
//                                lstNotNullField.add(strCurFieldName);
//                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstNotNullField;
    }

    protected ESMappingFieldModel createMappingField(String strFieldType, Boolean bIsDateField) {
        ESMappingFieldModel objMappingField = new ESMappingFieldModel();
        objMappingField.setType(null);
        objMappingField.setFielddata(null);
        objMappingField.setCopy_to(null);
        objMappingField.setIndex(null);

        if (bIsDateField) {
            objMappingField.setType("date");
        } else {
            if (strFieldType.equals(ESFilterOperationConstant.DATA_TYPE_TEXT)) {
                objMappingField.setType(ESFilterOperationConstant.DATA_TYPE_TEXT);
                objMappingField.setIndex(true);
            } else if (strFieldType.equals(ESFilterOperationConstant.DATA_TYPE_NUMERIC)) {
                objMappingField.setType("double");
            } else {
                objMappingField.setType(strFieldType);
            }
        }

        return objMappingField;
    }

    protected String getLatestIndexName(HashMap<String, String> mapIndexMapping, String strOldIndex) {
        String strLatestIndexName = strOldIndex;

        if (mapIndexMapping.containsKey(strOldIndex)) {
            strLatestIndexName = mapIndexMapping.get(strOldIndex);
            strLatestIndexName = getLatestIndexName(mapIndexMapping, strLatestIndexName);
        }

        return strLatestIndexName;
    }

    public List<Boolean> checkIndexExisted(String strIndex, String strType) {
        // Check index and type already existed or not
        List<ESIndexModel> lstIndex = getAllIndices();
        Boolean bIsExistsIndex = false;
        Boolean bIsExistsType = false;

        for (ESIndexModel objIndex : lstIndex) {
            if ((strIndex.contains("*") && objIndex.getIndex_name().contains(strIndex.replace("*", "")))
                || (!strIndex.contains("*") && objIndex.getIndex_name().equals(strIndex))) {
                bIsExistsIndex = true;

                for (String strIndexType : objIndex.getIndex_types()) {
                    if (strIndexType.equals(strType)) {
                        bIsExistsType = true;
                        break;
                    }
                }

                if (bIsExistsIndex) {
                    break;
                }
            }
        }

        List<Boolean> lstReturn = new ArrayList<>();
        lstReturn.add(bIsExistsIndex);
        lstReturn.add(bIsExistsType);

        return lstReturn;
    }

    public Boolean deleteIndex(String strIndex) {
        Boolean bIsDeleted = false;

        try {
            if (objESClient != null) {
                AcknowledgedResponse objDeleteResponse = objESClient.admin().indices().prepareDelete(strIndex).get();

                if (objDeleteResponse != null && objDeleteResponse.isAcknowledged()) {
                    bIsDeleted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsDeleted;
    }

    public Boolean updateSettingsOfIndex(String strIndex, HashMap<String, Integer> mapUpdateSetting) {
        Boolean bIsUpdated = false;

        try {
            if (objESClient != null && mapUpdateSetting != null && mapUpdateSetting.size() > 0) {
                Settings.Builder objBuilder = Settings.builder();

                for (Map.Entry<String, Integer> curSetting : mapUpdateSetting.entrySet()) {
                    objBuilder.put(curSetting.getKey(), curSetting.getValue());
                }

                AcknowledgedResponse objUpdateSettingResponse = objESClient.admin().indices().prepareUpdateSettings(strIndex)
                        .setSettings(objBuilder)
                        .get();

                if (objUpdateSettingResponse != null && objUpdateSettingResponse.isAcknowledged()) {
                    bIsUpdated = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("WARN: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsUpdated;
    }

    public Boolean createIndex(String strIndex, String strType, List<?> lstData, String strDateField,
                               HashMap<String, ESMappingFieldModel> mapMappingField, Boolean bDelIndexIfExisted, HashMap<String, String> mapFieldDataType) {
        Boolean bIsCreated = false;

        try {
            if (objESClient != null && lstData != null && lstData.size() > 0) {
                strIndex = strIndex.toLowerCase().trim();
                strType = strType.toLowerCase().trim();

                // Check index and type already existed or not
                List<ESIndexModel> lstIndex = getAllIndices();
                Boolean bIsExistsIndex = false;
                Boolean bIsExistsType = false;

                for (ESIndexModel objIndex : lstIndex) {
                    if (objIndex.getIndex_name().equals(strIndex)) {
                        bIsExistsIndex = true;

                        for (String strIndexType : objIndex.getIndex_types()) {
                            if (strIndexType.equals(strType)) {
                                bIsExistsType = true;
                                break;
                            }
                        }

                        if (bIsExistsIndex) {
                            break;
                        }
                    }
                }

                if (bIsExistsIndex && bDelIndexIfExisted) {
                    deleteIndex(strIndex);
                }

                // If not, create with mapping
                if (bIsExistsIndex && bIsExistsType) {
                    bIsCreated = true;
                } else {
                    // Convert first data item to JSON and convert back to HashMap
                    String strJSONData = objMapper.writeValueAsString(lstData.get(0));
                    Class<?> classZ = lstData.get(0).getClass();

                    HashMap<String, Object> objJSONData = (lstData.get(0) instanceof HashMap) ? (HashMap<String, Object>) lstData.get(0) : objMapper.readValue(strJSONData, HashMap.class);

                    objLogger.info("lstData(0): " + lstData.get(0));
                    objLogger.info("objJSONData: " + objJSONData);

                    Integer intCheck = 0;

                    if (lstData.get(0) instanceof HashMap) {
                        Long lTotalData = (long) lstData.size();
                        Long lCurData = 0l;
                        for (int intCount = 0; intCount < lstData.size(); intCount++) {
                            HashMap<String, Object> mapCur = (HashMap<String, Object>) lstData.get(0);

                            Long lTotalNull = mapCur.entrySet().stream().filter(objItem -> objItem.getValue() == null).count();

                            if (lTotalNull > 0) {
                                lCurData += 1;
                            } else {
                                intCheck = intCount;
                                break;
                            }
                        }

                        if (lCurData.equals(lTotalData)) {
                            intCheck = 0;
                        }
                    }

                    if (mapMappingField == null || mapMappingField.size() <= 0) {
                        mapMappingField = new HashMap<>();
                        Boolean bIsHashMap = false;

                        for (Map.Entry<String, Object> curItem : objJSONData.entrySet()) {
                            String strFieldType = "";
                            String strFieldName = curItem.getKey().replace(".", "-");

                            if (lstData.get(intCheck) instanceof HashMap) {
                                bIsHashMap = true;

                                if (mapFieldDataType == null || !mapFieldDataType.containsKey(curItem.getKey())) {
                                    try {
                                        Object objValue = ConverterUtil.convertStringToDataType(curItem.getValue().toString());
                                        objLogger.info("objValue: " + objValue);

                                        strFieldType = objValue.getClass().getCanonicalName().toLowerCase();
                                    } catch (Exception objEx) {
                                        strFieldType = "java.lang.string";
                                    }

                                } else {
                                    if (mapFieldDataType != null && mapFieldDataType.containsKey(curItem.getKey())) {
                                        strFieldType = mapFieldDataType.get(curItem.getKey()).toLowerCase();
                                    }
                                }
                            } else {
                                strFieldType = classZ.getDeclaredField(curItem.getKey()).getType().getTypeName()
                                        .toLowerCase();
                            }

                            if (strFieldType.isEmpty()) {
                                strFieldType = "java.lang.string";
                            }

                            objLogger.info("FieldType: " + curItem.getKey() + " - " + strFieldType);

                            ESMappingFieldModel objMappingField = new ESMappingFieldModel();
                            objMappingField.setType(null);
                            objMappingField.setFielddata(null);
                            objMappingField.setCopy_to(null);
                            objMappingField.setIndex(null);
                            objMappingField.setNorms(null);

                            if (curItem.getKey().toLowerCase().equals(strDateField.toLowerCase())) {
                                objMappingField.setType("date");
                            } else {
                                if (strFieldType.contains(".string")) {
                                    objMappingField.setType("keyword");
                                    objMappingField.setIndex(true);
                                } else if (strFieldType.contains(".calendar") || strFieldType.contains(".date")
                                        || strFieldType.contains(".time")) {
                                    objMappingField.setType("date");
                                } else if (strFieldType.contains(".bool")) {
                                    objMappingField.setType("boolean");
                                } else if (strFieldType.contains(".int")) {
                                    objMappingField.setType("integer");
                                } else if (strFieldType.contains(".long")) {
                                    objMappingField.setType("long");
                                } else if (strFieldType.contains(".double")) {
                                    objMappingField.setType("double");
                                } else if (strFieldType.contains(".byte")) {
                                    objMappingField.setType("byte");
                                } else if (strFieldType.contains(".float")) {
                                    objMappingField.setType("float");
                                } else if (strFieldType.contains(".short")) {
                                    objMappingField.setType("short");
                                }
                            }

                            //If type is not keyword or date, recheck again with whole data, if contain NA => type is keyword
                            if (!objMappingField.getType().equals("keyword") && !objMappingField.getType().equals("date") && bIsHashMap) {
                                Long lTotalNA = lstData.stream().map(objItem -> ((HashMap<String, Object>) objItem).get(curItem.getKey()))
                                        .filter(item -> item != null && JacksonFilter.checkNAString(item.toString())).count();

                                if (lTotalNA > 0) {
                                    objMappingField.setType("keyword");
                                    objMappingField.setIndex(true);
                                }
                            }

                            if (objMappingField.getType() != null) {
                                mapMappingField.put(strFieldName, objMappingField);
                            }
                        }
                    }

                    objLogger.info("mapMappingField: " + mapMappingField);
                    objLogger.info("mapMappingField-size: " + mapMappingField.size());

                    if (mapMappingField != null && mapMappingField.size() > 0) {
                        HashMap<String, HashMap<String, ESMappingFieldModel>> mapProperties = new HashMap<>();
                        mapProperties.put("properties", mapMappingField);

                        String strJSONMappingData = objMapper.writeValueAsString(mapProperties);
                        CreateIndexResponse objCreateIndexResponse = null;

                        if (!bIsExistsIndex) {
                            objLogger.info("createIndex: " + strIndex);

                            Settings.Builder objBuilder = Settings.builder()
                                    .put("index.mapping.total_fields.limit", mapMappingField.size() * 10)
                                    .put("index.max_result_window", 1000000000)
                                    .put("index.number_of_replicas", intNumReplica < 0 ? 0 : intNumReplica)
                                    .put("index.refresh_interval", "60s");

                            if (bIsUseHotWarm) {
                                objBuilder.put("index.routing.allocation.require.box_type", "hot");
                            }


                            objCreateIndexResponse = objESClient.admin().indices().prepareCreate(strIndex)
                                    .setSettings(objBuilder)
                                    .addMapping(strType, strJSONMappingData, XContentType.JSON)
                                    .get();

                            objLogger.info("objCreateIndexResponse: " + objCreateIndexResponse);
                        }

                        if (bIsExistsIndex
                                || (objCreateIndexResponse != null && objCreateIndexResponse.isAcknowledged())) {
                            AcknowledgedResponse objPutMappingResponse = objESClient.admin().indices()
                                    .preparePutMapping(strIndex).setType(strType)
                                    .setSource(strJSONMappingData, XContentType.JSON).get();

                            if (objPutMappingResponse != null && objPutMappingResponse.isAcknowledged()) {
                                try {
                                    HashMap<String, Object> mapSettings = new HashMap<>();
                                    mapSettings.put("script.max_compilations_rate", "10000/1m");
                                    objESClient.admin().cluster().prepareUpdateSettings().setTransientSettings(mapSettings).get();
                                } catch (Exception objEx) {
                                    objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
                                }

                                bIsCreated = true;
                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsCreated;
    }

    @SuppressWarnings("unchecked")
    public Boolean createIndex(String strIndex, String strType, List<?> lstData, String strDateField,
                               HashMap<String, ESMappingFieldModel> mapMappingField, Boolean bDelIndexIfExisted) {
        return createIndex(strIndex, strType, lstData, strDateField, mapMappingField, bDelIndexIfExisted, null);
    }

    public List<ESIndexModel> getAllIndices(String strIndexPattern, String strType) {
        List<ESIndexModel> lstIndices = new ArrayList<>();

        try {
            String strCheckIndexPattern = strIndexPattern.replace("*", "").trim();

            List<Object> lstClient = createESAdminClient();
            TransportClient objESClient = (TransportClient) lstClient.get(0);
            AdminClient objAdminClient = (AdminClient) lstClient.get(1);

            GetMappingsResponse objMappingResponse = objAdminClient.indices().getMappings(new GetMappingsRequest())
                    .get();

            if (objMappingResponse != null && objMappingResponse.getMappings() != null) {
                objMappingResponse.getMappings().forEach(curObject -> {
                    String strCurIndex = curObject.key;
                    List<String> lstCurType = new ArrayList<>();

                    curObject.value.forEach(curObjectType -> {
                        if (curObjectType.equals(strType)) {
                            lstCurType.add(curObjectType.key);
                        }
                    });

                    if (strCurIndex.contains(strCheckIndexPattern) && lstCurType != null && lstCurType.size() > 0) {
                        ESIndexModel objIndex = new ESIndexModel();
                        objIndex.setIndex_name(strCurIndex);
                        objIndex.setIndex_types(lstCurType);

                        lstIndices.add(objIndex);
                    }
                });
            }

            closeESClient(objESClient);
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstIndices;
    }

    public List<ESIndexModel> getAllIndices() {
        List<ESIndexModel> lstIndices = new ArrayList<>();

        try {
            List<Object> lstClient = createESAdminClient();
            TransportClient objESClient = (TransportClient) lstClient.get(0);
            AdminClient objAdminClient = (AdminClient) lstClient.get(1);

            GetMappingsResponse objMappingResponse = objAdminClient.indices().getMappings(new GetMappingsRequest())
                    .get();

            if (objMappingResponse != null && objMappingResponse.getMappings() != null) {
                objMappingResponse.getMappings().forEach(curObject -> {
                    String strCurIndex = curObject.key;
                    List<String> lstCurType = new ArrayList<>();

                    curObject.value.forEach(curObjectType -> {
                        lstCurType.add(curObjectType.key);
                    });

                    ESIndexModel objIndex = new ESIndexModel();
                    objIndex.setIndex_name(strCurIndex);
                    objIndex.setIndex_types(lstCurType);

                    lstIndices.add(objIndex);
                });
            }

            closeESClient(objESClient);
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstIndices;
    }

    public List<ESFieldModel> getFieldsMetaData(String strIndex, String strType, List<String> lstField, Boolean bIsCheckNull) {
        List<ESFieldModel> lstReturnField = new ArrayList<>();

        try {
            Map<String, Map<String, List<ESFieldModel>>> mapField = getFieldsOfIndices(Arrays.asList(strIndex),
                    Arrays.asList(strType), lstField, bIsCheckNull);

            if (mapField != null && mapField.containsKey(strIndex) && mapField.get(strIndex) != null
                    && mapField.get(strIndex).get(strType) != null) {
                lstReturnField = mapField.get(strIndex).get(strType);
            } else {
                String strIndexPattern = strIndex.replace("*", "");

                for (Map.Entry<String, Map<String, List<ESFieldModel>>> curEntry : mapField.entrySet()) {
                    if (curEntry.getKey().contains(strIndexPattern)) {
                        lstReturnField = curEntry.getValue().get(strType);
                        break;
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstReturnField;
    }

    public Boolean mergeDataFromIndices(MergingDataRequestModel objMergingRequest) {
        Boolean bIsMerged = false;

        String strScriptMergingIndex = generateMergingIDScript(objMergingRequest);

        if (strScriptMergingIndex != null && !strScriptMergingIndex.isEmpty()) {
            try {
                ReindexRequestBuilder objReindexReqBuilder = ReindexAction.INSTANCE.newRequestBuilder(objESClient)
                        .source(objMergingRequest.getIndices()
                                .toArray(new String[objMergingRequest.getIndices().size()]))
                        .destination(objMergingRequest.getNew_index_name())
                        .script(new Script(ScriptType.INLINE, "painless", strScriptMergingIndex, new HashMap<>()))
                        .timeout(TimeValue.MINUS_ONE);

                BulkByScrollResponse objResponse = objReindexReqBuilder.get();

                if (objResponse != null) {
                    objLogger.warn("INFO: " + objResponse.toString());
                    bIsMerged = true;
                }
            } catch (Exception objEx) {
                objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
            }
        }

        return bIsMerged;
    }

    protected void refreshIndex(String strIndex) {
        try {
            if (objESClient != null) {
                objESClient.admin().indices().refresh(new RefreshRequest(strIndex)).get();
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }
    }
}