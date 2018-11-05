package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import joptsimple.internal.Strings;
import org.chronotics.pithos.ext.es.log.Logger;
import org.chronotics.pithos.ext.es.log.LoggerFactory;
import org.chronotics.pithos.ext.es.model.*;
import org.chronotics.pithos.ext.es.util.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticConnection {
    private Logger objLogger = LoggerFactory.getLogger(ElasticConnection.class);
    String strESClusterName = "";
    String strESCoorNodeIP = "";
    Integer intESCoorNodePort = 0;
    Integer intNumBulkOperation = 20000;
    TransportClient objESClient;
    private List<String> lstConvertedDataType = new ArrayList<>();

    public static ElasticConnection instance;

    private ObjectMapper objMapper = new ObjectMapper();

    public ElasticConnection(String strESClusterName, String strESCoorNodeIP, Integer intESCoorNodePort) {
        this.strESClusterName = strESClusterName;
        this.strESCoorNodeIP = strESCoorNodeIP;
        this.intESCoorNodePort = intESCoorNodePort;

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
                    instance = new ElasticConnection(strESClusterName, strESCoorNodeIP, intESCoorNodePort);
                }
            }
        }

        return instance;
    }

    @SuppressWarnings("resource")
    private TransportClient createESClient() {
        TransportClient objESClient = null;

        try {
            Settings objSetting = Settings.builder().put("cluster.name", strESClusterName)
                    .put("client.transport.sniff", false).build();
            objESClient = new PreBuiltTransportClient(objSetting, MatrixAggregationPlugin.class).addTransportAddress(
                    new TransportAddress(InetAddress.getByName(strESCoorNodeIP), intESCoorNodePort));
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objESClient;
    }

    public void closeInstance() {
        try {
            if (objESClient != null) {
                objESClient.close();
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }
    }

    private List<Object> createESAdminClient() {
        List<Object> lstClient = new ArrayList<>();

        AdminClient objClient = null;
        try {
            objClient = objESClient.admin();

            lstClient.add(objESClient);
            lstClient.add(objClient);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstClient;
    }

    private IndicesAdminClient createESIndiceAdminClient() {
        IndicesAdminClient objClient = null;

        try {
            objClient = objESClient.admin().indices();
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objClient;
    }

    private BulkProcessor createBulkProcessor(TransportClient objESClient, Integer intDataSize) {
        BulkProcessor objBulkProcessor = BulkProcessor.builder(objESClient, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                objLogger.info("INFO: After Bulk - " + String.valueOf(l) + " - "
                        + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                objLogger.error("ERR: After Bulk with Throwable - " + String.valueOf(l) + " - "
                        + bulkRequest.numberOfActions());
                objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(throwable));
            }
        }).setBulkActions(intDataSize < intNumBulkOperation ? intDataSize : intNumBulkOperation)
                .build();

        return objBulkProcessor;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, List<ESFieldModel>>> getFieldsOfIndices(List<String> lstIndex, List<String> lstType,
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

                        // Make sure the list of Fields doesnt contain any empty field
                        List<String> notNullField = getNotNullField(strCurIndex, strCurType, lstField);
                        List<ESFieldModel> lstNotNullESField = new ArrayList<>();
                        for (ESFieldModel fd : lstESField) {
                            if (!bIsCheckNull || notNullField.contains(fd.getFull_name())) {
                                lstNotNullESField.add(fd);
                            }
                        }
                        mapType.put(strCurType, lstNotNullESField);
                    }

                    mapFields.put(strCurIndex, mapType);
                }
            }

        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapFields;
    }

    private ESQueryResultModel getResponseDataFromQueryByFieldIdxAndRowIdx(String strIndex, String strType,
                                                                           ESFilterAllRequestModel objFilterAllRequest, List<String> lstSelectedField, Integer intFromRow,
                                                                           Integer intNumRow, Integer intFromCol, Integer intNumCol, Boolean bIsSimpleStats) {
        ESQueryResultModel objQueryResult = new ESQueryResultModel();
        SearchResponse objSearchResponse = new SearchResponse();

        try {
            Map<String, Map<String, List<ESFieldModel>>> mapFieldOfIndex = getFieldsOfIndices(Arrays.asList(strIndex),
                    Arrays.asList(strType), null, true);

            if (mapFieldOfIndex != null && mapFieldOfIndex.size() > 0 && mapFieldOfIndex.containsKey(strIndex)) {
                if (mapFieldOfIndex.get(strIndex) != null && mapFieldOfIndex.get(strIndex).size() > 0
                        && mapFieldOfIndex.get(strIndex).containsKey(strType)) {
                    List<ESFieldModel> lstFieldModel = mapFieldOfIndex.get(strIndex).get(strType);
                    List<String> lstSourceField = new ArrayList<>();
                    List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                            && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                            ? objFilterAllRequest.getFilters()
                            : new ArrayList<ESFilterRequestModel>();

                    Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

                    objLogger.info("lstSelectedField: " + lstSelectedField);

                    if (lstSelectedField == null || lstSelectedField.size() <= 0) {
                        if (intNumCol > 0) {
                            lstSourceField = lstFieldModel
                                    .subList(intFromCol,
                                            (intFromCol + intNumCol) > lstFieldModel.size() ? lstFieldModel.size()
                                                    : (intFromCol + intNumCol))
                                    .stream().map(objField -> objField.getFull_name()).collect(Collectors.toList());
                        } else {
                            lstSourceField = lstFieldModel.stream().map(objField -> objField.getFull_name())
                                    .collect(Collectors.toList());
                        }
                    } else {
                        lstSourceField = new ArrayList<>(lstSelectedField);
                    }

                    if (lstSourceField != null && lstSourceField.size() > 0) {
                        if (objFilterAllRequest != null) {
                            objSearchResponse = getResponseDataFromQuery(new String[]{strIndex},
                                    new String[]{strType}, lstSourceField.toArray(new String[lstSourceField.size()]),
                                    lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, objFilterAllRequest.getDeleted_rows());
                        } else {
                            objSearchResponse = getResponseDataFromQuery(new String[]{strIndex},
                                    new String[]{strType}, lstSourceField.toArray(new String[lstSourceField.size()]),
                                    lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, new ArrayList<>());
                        }

                        lstSourceField.add("_id");

                        objQueryResult.setSearch_response(objSearchResponse);
                        objQueryResult.setTotal_fields(lstFieldModel.size() + 1);
                        objQueryResult.setNum_selected_fields(lstSourceField.size());
                        objQueryResult.setSelected_fields(lstSourceField);

                        if ((objFilterAllRequest == null || objFilterAllRequest.getFilters() == null
                                || objFilterAllRequest.getFilters().size() <= 0) && intFromRow == 0) {
                            List<ESFieldAggModel> lstFieldAggs = getHistogramOfField(strIndex, strType, lstSourceField,
                                    bIsSimpleStats);
                            objQueryResult.setAgg_fields(lstFieldAggs);
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objQueryResult;
    }

    private BoolQueryBuilder generateAggQueryBuilder(String strIndex, String strType, BoolQueryBuilder objQueryBuilder,
                                                     List<ESFilterRequestModel> lstNotAddedFilterRequest, List<ESFieldModel> lstFieldModel) {
        List<String> lstNotAddedFieldName = lstNotAddedFilterRequest.stream()
                .filter(objFilter -> (objFilter.getFiltered_conditions() != null
                        && objFilter.getFiltered_conditions().size() > 0))
                .map(objFiltered -> objFiltered.getFiltered_on_field()).collect(Collectors.toList());

        List<String> lstNumericField = new ArrayList<>();
        List<String> lstTextField = new ArrayList<>();

        lstNumericField = lstFieldModel.stream()
                .filter(objField -> !objField.getType().equals("text") && !objField.getType().equals("keyword") && !objField.getType().equals("date")  && lstNotAddedFieldName.contains(objField.getFull_name()))
                .map(objField -> objField.getFull_name())
                .collect(Collectors.toList());

        lstTextField = lstFieldModel.stream()
                .filter(objField -> lstNotAddedFieldName.contains(objField.getFull_name()) && (objField.getType().equals("text") || objField.getType().equals("keyword")))
                .map(objField -> objField.getFull_name())
                .collect(Collectors.toList());

        Map<String, ESFieldStatModel> mapStats = statsField(strIndex, strType, lstNumericField, lstTextField, false);

        for (int intCountRequest = 0; intCountRequest < lstNotAddedFilterRequest.size(); intCountRequest++) {
            ESFilterRequestModel objCurFilterRequest = lstNotAddedFilterRequest.get(intCountRequest);

            String strFieldName = ESFilterConverterUtil.checkFieldName(objCurFilterRequest, lstFieldModel);

            if (strFieldName != null && !strFieldName.isEmpty() && objCurFilterRequest.getFiltered_conditions() != null
                    && objCurFilterRequest.getFiltered_conditions().size() > 0 && mapStats.containsKey(strFieldName)) {
                String strFieldCondition = objCurFilterRequest.getFiltered_conditions().get(0);
                ESFieldStatModel objFieldStat = mapStats.get(strFieldName);
                Double dbFromValue = 0.0;
                Double dbToValue = 0.0;
                Boolean bIsCheckOutlier = false;

                switch (strFieldCondition) {
                    case ESFilterOperationConstant.FILTER_LCL:
                        dbFromValue = objFieldStat.getMin();
                        dbToValue = objFieldStat.getLcl();
                        break;
                    case ESFilterOperationConstant.FILTER_UCL:
                        dbFromValue = objFieldStat.getUcl();
                        dbToValue = objFieldStat.getMax();
                        break;
                    case ESFilterOperationConstant.FILTER_LCL_UCL:
                        dbFromValue = objFieldStat.getLcl();
                        dbToValue = objFieldStat.getUcl();
                        break;
                    case ESFilterOperationConstant.FILTER_OUTLIER_MILD:
                        bIsCheckOutlier = true;
                        dbFromValue = objFieldStat.getLower_inner_fence();
                        dbToValue = objFieldStat.getUpper_inner_fence();
                        break;
                    case ESFilterOperationConstant.FILTER_OUTLIER_EXTREME:
                        bIsCheckOutlier = true;
                        dbFromValue = objFieldStat.getLower_outer_fence();
                        dbToValue = objFieldStat.getUpper_outer_fence();
                        break;
                }

                switch (objCurFilterRequest.getFiltered_operation()) {
                    case ESFilterOperationConstant.IS:
                        if (bIsCheckOutlier) {
                            objQueryBuilder.mustNot(
                                    QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        } else {
                            objQueryBuilder.must(
                                    QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        }

                        break;
                    case ESFilterOperationConstant.IS_NOT:
                        if (bIsCheckOutlier) {
                            objQueryBuilder.must(
                                    QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        } else {
                            objQueryBuilder.mustNot(
                                    QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        }
                        break;
                    case ESFilterOperationConstant.IS_BETWEEN:
                        objQueryBuilder
                                .must(QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        break;
                    case ESFilterOperationConstant.IS_NOT_BETWEEN:
                        objQueryBuilder.mustNot(
                                QueryBuilders.rangeQuery(strFieldName).from(dbFromValue, true).to(dbToValue, true));
                        break;
                }
            }
        }

        return objQueryBuilder;
    }

    @SuppressWarnings("unchecked")
    private SearchResponse getResponseDataFromQuery(String[] arrIndex, String[] arrType, String[] arrSource,
                                                    List<ESFilterRequestModel> lstFilterRequest, Boolean bIsReversedFilter, Integer intFrom, Integer intSize,
                                                    List<ESFieldModel> lstFieldModel, List<String> lstDeletedRows) {
        SearchResponse objSearchResponse = new SearchResponse();

        try {
            SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(arrIndex).setTypes(arrType)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

            SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
            objSearchSourceBuilder.size(intSize).from(intFrom).sort("_doc");

            if (lstFilterRequest != null && lstFilterRequest.size() > 0) {
                List<Object> lstReturn = ESFilterConverterUtil.createBooleanQueryBuilders(lstFilterRequest, lstFieldModel, lstDeletedRows, bIsReversedFilter);
                BoolQueryBuilder objQueryBuilder = (BoolQueryBuilder) lstReturn.get(0);

                List<ESFilterRequestModel> lstNotAddedFilterRequest = (List<ESFilterRequestModel>) lstReturn.get(1);

                if (objQueryBuilder != null) {
                    // Special case: to get value from UCL, LCL, must get statistic information first
                    if (lstNotAddedFilterRequest != null && lstNotAddedFilterRequest.size() > 0) {
                        objQueryBuilder = generateAggQueryBuilder(arrIndex[0], arrType[0], objQueryBuilder,
                                lstNotAddedFilterRequest, lstFieldModel);
                    }

                    objSearchSourceBuilder.query(objQueryBuilder);
                }
            }

            if (arrSource != null && arrSource.length > 0) {
                objSearchSourceBuilder.fetchSource(arrSource, null);
            }

            objRequestBuilder.setSource(objSearchSourceBuilder);
            objSearchResponse = objRequestBuilder.get();

            closeESClient(objESClient);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objSearchResponse;
    }

    private HashMap<String, List<Double>> statsNullityOfField(String strIndex, String strType, List<String> lstField) {
        HashMap<String, List<Double>> mapNullity = new HashMap<>();

        try {
            if (objESClient != null) {
                Long lTotalHit = 0L;

                //Get Total Hit First
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);

                MatchAllQueryBuilder objMatchAllQuery = new MatchAllQueryBuilder();
                SearchResponse objSearchResponse = objSearchRequestBuilder.setQuery(objMatchAllQuery).get();

                if (objSearchResponse != null && objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() >= 0) {
                    lTotalHit = objSearchResponse.getHits().getTotalHits();
                }

                for (int intCount = 0; intCount < lstField.size(); intCount++) {
                    String strField = lstField.get(intCount);

                    objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                    SearchSourceBuilder objSearchSoureBuilder = new SearchSourceBuilder();
                    objSearchSoureBuilder.size(0);

                    BoolQueryBuilder objBoolQuery = new BoolQueryBuilder();
                    objBoolQuery.mustNot(QueryBuilders.existsQuery(strField));

                    objSearchSoureBuilder.query(objBoolQuery);
                    objSearchRequestBuilder.setSource(objSearchSoureBuilder);

                    SearchResponse objNullResponse = objSearchRequestBuilder.get();

                    if (objNullResponse != null && objNullResponse.getHits() != null && objNullResponse.getHits().getTotalHits() > 0) {
                        Long lCurHit = objNullResponse.getHits().getTotalHits();

                        mapNullity.put(strField, Arrays.asList(lCurHit.doubleValue(), (lCurHit.doubleValue() * 100) / lTotalHit));
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapNullity;
    }

    // Way to generate Histogram of array data:
    // http://www.oswego.edu/~srp/stats/hist_con.htm
    private List<ESFieldAggModel> getHistogramOfField(String strIndex, String strType, List<String> lstField,
                                                      Boolean bIsSimpleStats) {
        List<ESFieldAggModel> lstAggResult = new ArrayList<>();
        Map<String, List<ESFieldPointModel>> mapHistogramPoint = new HashMap<>();
        Map<String, ESFieldStatModel> mapFieldStats = new HashMap<>();

        try {
            // Get data type of fields
            List<ESFieldModel> lstFieldMeta = getFieldsMetaData(strIndex, strType, lstField, true);

            List<String> lstTextField = lstFieldMeta.stream()
                    .filter(objField -> objField.getType().equals("keyword")
                            || (objField.getType().equals("text") && objField.getFielddata()))
                    .map(objFilteredField -> objFilteredField.getFull_name()).collect(Collectors.toList());
            List<String> lstNumberField = lstFieldMeta.stream()
                    .filter(objField -> !objField.getType().equals("keyword") && !objField.getType().equals("text")
                            && !objField.getType().equals("date"))
                    .map(objFilteredField -> objFilteredField.getFull_name()).collect(Collectors.toList());

            List<String> lstAllField = lstFieldMeta.stream().map(objField -> objField.getFull_name()).collect(Collectors.toList());

            // If fields are number, get histogram
            // Get min, max of fields
            // Calculate interval of each fields
            // Get Histogram
            if (!bIsSimpleStats) {
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                Map<String, ArrayList<Double>> mapFieldMinMax = new HashMap<>();
                Map<String, Double> mapFieldInterval = new HashMap<>();

                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.size(0);
                objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                for (int intCount = 0; intCount < lstNumberField.size(); intCount++) {
                    String strMinName = lstNumberField.get(intCount) + "_min";
                    String strMaxName = lstNumberField.get(intCount) + "_max";

                    objSearchRequestBuilder
                            .addAggregation(AggregationBuilders.min(strMinName).field(lstNumberField.get(intCount)));
                    objSearchRequestBuilder
                            .addAggregation(AggregationBuilders.max(strMaxName).field(lstNumberField.get(intCount)));
                }

                SearchResponse objMinMaxSearchResponse = objSearchRequestBuilder.get();
                Double dbTotalHit = 0.0;

                if (objMinMaxSearchResponse != null && objMinMaxSearchResponse.getAggregations() != null
                        && objMinMaxSearchResponse.getAggregations().asList() != null) {
                    dbTotalHit = new Long(objMinMaxSearchResponse.getHits().getTotalHits()).doubleValue();

                    List<Aggregation> lstMinMaxAggs = objMinMaxSearchResponse.getAggregations().asList();

                    for (int intCount = 0; intCount < lstMinMaxAggs.size(); intCount++) {
                        String strCurFieldName = lstMinMaxAggs.get(intCount).getName().replace("_max", "")
                                .replace("_min", "").trim();

                        if (lstMinMaxAggs.get(intCount).getName().contains("_min")) {
                            Double dbMin = ((InternalMin) lstMinMaxAggs.get(intCount)).getValue();
                            mapFieldMinMax.put(strCurFieldName, new ArrayList<>(Arrays.asList(dbMin)));
                        }
                    }

                    for (int intCount = 0; intCount < lstMinMaxAggs.size(); intCount++) {
                        String strCurFieldName = lstMinMaxAggs.get(intCount).getName().replace("_max", "")
                                .replace("_min", "").trim();

                        if (lstMinMaxAggs.get(intCount).getName().contains("_max")) {
                            Double dbMax = ((InternalMax) lstMinMaxAggs.get(intCount)).getValue();

                            if (mapFieldMinMax.containsKey(strCurFieldName)) {
                                mapFieldMinMax.get(strCurFieldName).add(dbMax);
                            }
                        }
                    }

                    Double dbSqrtTotalHit = Math.sqrt(dbTotalHit);

                    for (Map.Entry<String, ArrayList<Double>> curMinMax : mapFieldMinMax.entrySet()) {
                        if (curMinMax.getValue() != null && curMinMax.getValue().size() > 1) {
                            Double dbInterval = curMinMax.getValue().get(1) - curMinMax.getValue().get(0);
                            dbInterval = dbInterval / dbSqrtTotalHit;

                            mapFieldInterval.put(curMinMax.getKey(), dbInterval);
                        }
                    }

                    if (mapFieldInterval != null && mapFieldInterval.size() > 0) {
                        objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                        objSearchSourceBuilder = new SearchSourceBuilder();
                        objSearchSourceBuilder.size(0);
                        objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                        for (Map.Entry<String, Double> curFieldInterval : mapFieldInterval.entrySet()) {
                            objSearchRequestBuilder
                                    .addAggregation(AggregationBuilders.histogram(curFieldInterval.getKey() + "_hist")
                                            .interval(curFieldInterval.getValue()).field(curFieldInterval.getKey())
                                            .minDocCount(1L));
                        }

                        SearchResponse objHistogramResponse = objSearchRequestBuilder.get();

                        if (objHistogramResponse != null && objHistogramResponse.getAggregations() != null
                                && objHistogramResponse.getAggregations().asList() != null) {
                            List<Aggregation> lstHistAgg = objHistogramResponse.getAggregations().asList();
                            for (int intCount = 0; intCount < lstHistAgg.size(); intCount++) {
                                String strCurFieldName = lstHistAgg.get(intCount).getName().replace("_hist", "").trim();

                                if (lstHistAgg.get(intCount).getName().contains("_hist")) {
                                    InternalHistogram objCurHist = (InternalHistogram) lstHistAgg.get(intCount);

                                    if (objCurHist != null && objCurHist.getBuckets() != null
                                            && objCurHist.getBuckets().size() > 0) {
                                        List<ESFieldPointModel> lstHistPoint = new ArrayList<>();

                                        for (int intCountBucket = 0; intCountBucket < objCurHist.getBuckets()
                                                .size(); intCountBucket++) {
                                            ESFieldPointModel objPoint = new ESFieldPointModel();
                                            objPoint.setPoint_value(new Double(
                                                    objCurHist.getBuckets().get(intCountBucket).getKey().toString()));
                                            objPoint.setPoint_num(
                                                    objCurHist.getBuckets().get(intCountBucket).getDocCount());
                                            objPoint.setPoint_percent(
                                                    objCurHist.getBuckets().get(intCountBucket).getDocCount() * 100
                                                            / dbTotalHit);

                                            lstHistPoint.add(objPoint);
                                        }

                                        mapHistogramPoint.put(strCurFieldName, lstHistPoint);
                                    }
                                }
                            }
                        }
                    }
                }

                if (lstTextField != null && lstTextField.size() > 0) {
                    // If fields are text, get top hits
                    objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                    objSearchSourceBuilder = new SearchSourceBuilder();
                    objSearchSourceBuilder.size(0);
                    objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                    if (dbTotalHit == null || dbTotalHit <= 0.0) {
                        objSearchRequestBuilder.addAggregation(
                                AggregationBuilders.count(lstTextField.get(0) + "_count").field(lstTextField.get(0)));

                        SearchResponse objCountResponse = objSearchRequestBuilder.get();

                        if (objCountResponse != null && objCountResponse.getAggregations() != null
                                && objCountResponse.getAggregations().asList() != null
                                && objCountResponse.getAggregations().asList().size() > 0) {
                            ValueCount objCountAgg = (ValueCount) objCountResponse.getAggregations().asList().get(0);

                            if (objCountAgg != null) {
                                dbTotalHit = objCountAgg.value();
                            }
                        }
                    }

                    for (int intCount = 0; intCount < lstTextField.size(); intCount++) {
                        objSearchRequestBuilder.addAggregation(AggregationBuilders
                                .terms(lstTextField.get(intCount) + "_tophits").field(lstTextField.get(intCount))
                                .size(new Double(Math.sqrt(dbTotalHit)).intValue()));
                    }

                    SearchResponse objTermResponse = objSearchRequestBuilder.get();

                    if (objTermResponse != null && objTermResponse.getAggregations() != null
                            && objTermResponse.getAggregations().asList() != null) {
                        List<Aggregation> lstTermAgg = objTermResponse.getAggregations().asList();
                        for (int intCount = 0; intCount < lstTermAgg.size(); intCount++) {
                            String strCurFieldName = lstTermAgg.get(intCount).getName().replace("_tophits", "").trim();

                            if (lstTermAgg.get(intCount).getName().contains("_tophits")) {
                                StringTerms objCurTerm = (StringTerms) lstTermAgg.get(intCount);

                                if (objCurTerm != null && objCurTerm.getBuckets() != null
                                        && objCurTerm.getBuckets().size() > 0) {
                                    List<ESFieldPointModel> lstHistPoint = new ArrayList<>();

                                    for (int intCountBucket = 0; intCountBucket < objCurTerm.getBuckets()
                                            .size(); intCountBucket++) {
                                        ESFieldPointModel objPoint = new ESFieldPointModel();
                                        objPoint.setPoint_key(
                                                objCurTerm.getBuckets().get(intCountBucket).getKeyAsString());
                                        objPoint.setPoint_num(
                                                objCurTerm.getBuckets().get(intCountBucket).getDocCount());
                                        objPoint.setPoint_percent(
                                                objCurTerm.getBuckets().get(intCountBucket).getDocCount() * 100
                                                        / dbTotalHit);
                                        lstHistPoint.add(objPoint);
                                    }

                                    mapHistogramPoint.put(strCurFieldName, lstHistPoint);
                                }
                            }
                        }
                    }
                }
            }

            mapFieldStats = statsField(strIndex, strType, lstNumberField, lstTextField, bIsSimpleStats);

            closeESClient(objESClient);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        if (mapHistogramPoint != null && mapHistogramPoint.size() > 0) {
            for (Map.Entry<String, List<ESFieldPointModel>> curField : mapHistogramPoint.entrySet()) {
                ESFieldAggModel objFieldAggModel = new ESFieldAggModel();
                objFieldAggModel.setField(curField.getKey());
                objFieldAggModel.setData_points(curField.getValue());

                if (mapFieldStats.containsKey(curField.getKey())) {
                    objFieldAggModel.setField_stats(mapFieldStats.get(curField.getKey()));
                }

                lstAggResult.add(objFieldAggModel);
            }
        } else if (mapFieldStats != null && mapFieldStats.size() > 0) {
            for (Map.Entry<String, ESFieldStatModel> curField : mapFieldStats.entrySet()) {
                ESFieldAggModel objFieldAggModel = new ESFieldAggModel();
                objFieldAggModel.setField(curField.getKey());
                objFieldAggModel.setField_stats(mapFieldStats.get(curField.getKey()));

                lstAggResult.add(objFieldAggModel);
            }
        }

        return lstAggResult;
    }

    private Map<String, List<Double>> statsMismatchOfField(String strIndex, String strType, List<String> lstStringField) {
        Map<String, List<Double>> mapStats = new HashMap<>();

        try {
            if (objESClient != null && lstStringField != null && lstStringField.size() > 0) {
                Long lTotalHit = 0L;

                //Check nullity
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.size(0);
                objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                //Get Total Hit First
                MatchAllQueryBuilder objMatchAllQuery = new MatchAllQueryBuilder();
                SearchResponse objSearchResponse = objSearchRequestBuilder.setQuery(objMatchAllQuery).get();

                if (objSearchResponse != null && objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() >= 0) {
                    lTotalHit = objSearchResponse.getHits().getTotalHits();
                }

                if (lTotalHit > 0) {
                    for (int intCount = 0; intCount < lstStringField.size(); intCount++) {
                        try {
                            BoolQueryBuilder objBooleanQuery = new BoolQueryBuilder();
                            TermQueryBuilder objNAQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "NA");
                            TermQueryBuilder objnaQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "na");
                            TermQueryBuilder objN_AQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "N/A");
                            TermQueryBuilder objn_aQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "n/a");
                            TermQueryBuilder objNANQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "NAN");
                            TermQueryBuilder objnanQueryBuilder = QueryBuilders.termQuery(lstStringField.get(intCount), "nan");
                            objBooleanQuery.should(objNAQueryBuilder).should(objnaQueryBuilder)
                                    .should(objN_AQueryBuilder).should(objn_aQueryBuilder)
                                    .should(objNANQueryBuilder).should(objnanQueryBuilder);

                            objSearchRequestBuilder.setQuery(null);

                            objSearchResponse = objSearchRequestBuilder.setQuery(objBooleanQuery).get();

                            if (objSearchResponse != null && objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() >= 0) {
                                Long lCurHit = objSearchResponse.getHits().getTotalHits();

                                List<Double> lstNullityStats = new ArrayList<>();
                                lstNullityStats.add(lCurHit.doubleValue());
                                lstNullityStats.add(lCurHit.doubleValue() * 100.0 / lTotalHit);

                                mapStats.put(lstStringField.get(intCount), lstNullityStats);
                            }
                        } catch (Exception objEx) {
                            objLogger.warn("WARN: " + ExceptionUtil.getStrackTrace(objEx));
                        }
                    }

                    //Check mismatch
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapStats;
    }

    // Way to calculate Outlier:
    // https://www.itl.nist.gov/div898/handbook/prc/section1/prc16.htm
    private Map<String, ESFieldStatModel> statsField(String strIndex, String strType, List<String> lstNumberField, List<String> lstStringField,
                                                     Boolean bIsSimpleStats) {
        Map<String, ESFieldStatModel> mapFieldStat = new HashMap<>();

        try {
            if (objESClient != null) {
                Map<String, List<Double>> mapMismatchStats = statsMismatchOfField(strIndex, strType, lstStringField);

                List<String> lstCombineField = new ArrayList<>();

                if (lstNumberField != null && lstNumberField.size() > 0) {
                    for (int intCount = 0; intCount < lstNumberField.size(); intCount++) {
                        lstCombineField.add(lstNumberField.get(intCount));
                    }
                }

                if (lstStringField != null && lstStringField.size() > 0) {
                    for (int intCount = 0; intCount < lstStringField.size(); intCount++) {
                        lstCombineField.add(lstStringField.get(intCount));
                    }
                }

                Map<String, List<Double>> mapNullStats = statsNullityOfField(strIndex, strType, lstCombineField);

                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.size(0);
                objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                for (int intCount = 0; intCount < lstNumberField.size(); intCount++) {
                    String strStatName = lstNumberField.get(intCount) + "_summary";
                    Map<String, String> mapCurBucketPath = new HashMap<>();
                    Script objCurScript = null;

                    AggregationBuilder objCurAggBuilder = AggregationBuilders.terms(strStatName)
                            .script(new Script("'" + strStatName + "'"));

                    AggregationBuilder objCurExtendedStatsAggBuilder = AggregationBuilders
                            .extendedStats(lstNumberField.get(intCount) + "_stats").field(lstNumberField.get(intCount));
                    AggregationBuilder objCurPercentilesAggBuilder = AggregationBuilders
                            .percentiles(lstNumberField.get(intCount) + "_percentiles").percentiles(25.0, 50.0, 75.0)
                            .field(lstNumberField.get(intCount));

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("mean", lstNumberField.get(intCount) + "_stats.avg");
                    mapCurBucketPath.put("std", lstNumberField.get(intCount) + "_stats.std_deviation");
                    objCurScript = new Script("params.mean + 3 * params.std");

                    PipelineAggregationBuilder objCurUCLAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_ucl", mapCurBucketPath, objCurScript);

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("mean", lstNumberField.get(intCount) + "_stats.avg");
                    mapCurBucketPath.put("std", lstNumberField.get(intCount) + "_stats.std_deviation");
                    objCurScript = new Script("params.mean - 3 * params.std");

                    PipelineAggregationBuilder objCurLCLAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_lcl", mapCurBucketPath, objCurScript);

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("q1", lstNumberField.get(intCount) + "_percentiles[25.0]");
                    mapCurBucketPath.put("q3", lstNumberField.get(intCount) + "_percentiles[75.0]");
                    objCurScript = new Script("params.q1 - 1.5 * (params.q3 - params.q1)");

                    PipelineAggregationBuilder objLowerInnerFenceAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_lif", mapCurBucketPath, objCurScript);

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("q1", lstNumberField.get(intCount) + "_percentiles[25.0]");
                    mapCurBucketPath.put("q3", lstNumberField.get(intCount) + "_percentiles[75.0]");
                    objCurScript = new Script("params.q3 + 1.5 * (params.q3 - params.q1)");

                    PipelineAggregationBuilder objUpperInnerFenceAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_uif", mapCurBucketPath, objCurScript);

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("q1", lstNumberField.get(intCount) + "_percentiles[25.0]");
                    mapCurBucketPath.put("q3", lstNumberField.get(intCount) + "_percentiles[75.0]");
                    objCurScript = new Script("params.q1 - 3.0 * (params.q3 - params.q1)");

                    PipelineAggregationBuilder objLowerOuterFenceAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_lof", mapCurBucketPath, objCurScript);

                    mapCurBucketPath = new HashMap<>();
                    mapCurBucketPath.put("q1", lstNumberField.get(intCount) + "_percentiles[25.0]");
                    mapCurBucketPath.put("q3", lstNumberField.get(intCount) + "_percentiles[75.0]");
                    objCurScript = new Script("params.q3 + 3.0 * (params.q3 - params.q1)");

                    PipelineAggregationBuilder objUpperOuterFenceAggBuilder = PipelineAggregatorBuilders
                            .bucketScript(lstNumberField.get(intCount) + "_uof", mapCurBucketPath, objCurScript);

                    if (bIsSimpleStats) {
                        objCurAggBuilder = objCurAggBuilder.subAggregation(objCurExtendedStatsAggBuilder);
                    } else {
                        objCurAggBuilder = objCurAggBuilder.subAggregation(objCurExtendedStatsAggBuilder)
                                .subAggregation(objCurPercentilesAggBuilder).subAggregation(objCurUCLAggBuilder)
                                .subAggregation(objCurLCLAggBuilder).subAggregation(objLowerInnerFenceAggBuilder)
                                .subAggregation(objUpperInnerFenceAggBuilder)
                                .subAggregation(objLowerOuterFenceAggBuilder)
                                .subAggregation(objUpperOuterFenceAggBuilder);
                    }

                    objSearchRequestBuilder.addAggregation(objCurAggBuilder);
                }

                SearchResponse objStatsResponse = objSearchRequestBuilder.get();

                if (objStatsResponse != null && objStatsResponse.getAggregations() != null
                        && objStatsResponse.getAggregations().asList() != null) {
                    List<Aggregation> lstStatAggs = objStatsResponse.getAggregations().asList();

                    for (int intCount = 0; intCount < lstStatAggs.size(); intCount++) {
                        String strCurFieldName = lstStatAggs.get(intCount).getName().replace("_summary", "");

                        if (lstStatAggs.get(intCount).getName().contains("_summary")) {
                            StringTerms objCurStats = (StringTerms) lstStatAggs.get(intCount);

                            if (objCurStats != null && objCurStats.getBuckets() != null
                                    && objCurStats.getBuckets().size() > 0) {
                                if (objCurStats.getBuckets().get(0).getAggregations() != null
                                        && objCurStats.getBuckets().get(0).getAggregations().asList().size() > 0) {
                                    List<Aggregation> lstSubAgg = objCurStats.getBuckets().get(0).getAggregations()
                                            .asList();
                                    ESFieldStatModel objCurStatsField = new ESFieldStatModel();

                                    for (int intCountSubAggs = 0; intCountSubAggs < lstSubAgg
                                            .size(); intCountSubAggs++) {
                                        if (lstSubAgg.get(intCountSubAggs).getName().contains("_stats")) {
                                            InternalExtendedStats objSubStats = (InternalExtendedStats) lstSubAgg
                                                    .get(intCountSubAggs);

                                            objCurStatsField.setAvg(objSubStats.getAvg());
                                            objCurStatsField.setCount(objSubStats.getCount());
                                            objCurStatsField.setMax(objSubStats.getMax());
                                            objCurStatsField.setMin(objSubStats.getMin());
                                            objCurStatsField.setStd_deviation(objSubStats.getStdDeviation());
                                            objCurStatsField.setSum(objSubStats.getSum());
                                            objCurStatsField.setSum_of_squares(objSubStats.getSumOfSquares());
                                            objCurStatsField.setVariance(objSubStats.getVariance());
                                            objCurStatsField.setSigma(objSubStats.getSigma());
                                        }

                                        if (!bIsSimpleStats) {
                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_percentiles")) {
                                                InternalTDigestPercentiles objSubStats = (InternalTDigestPercentiles) lstSubAgg
                                                        .get(intCountSubAggs);

                                                if (objSubStats != null) {
                                                    objCurStatsField.setPercentile_25th(objSubStats.percentile(25.0));
                                                    objCurStatsField.setPercentile_50th(objSubStats.percentile(50.0));
                                                    objCurStatsField.setPercentile_75th(objSubStats.percentile(75.0));
                                                }
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_ucl")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setUcl(objSubStats.getValue());
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_lcl")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setLcl(objSubStats.getValue());
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_lif")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setLower_inner_fence(objSubStats.getValue());
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_uif")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setUpper_inner_fence(objSubStats.getValue());
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_lof")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setLower_outer_fence(objSubStats.getValue());
                                            }

                                            if (lstSubAgg.get(intCountSubAggs).getName().contains("_uof")) {
                                                InternalSimpleValue objSubStats = (InternalSimpleValue) lstSubAgg
                                                        .get(intCountSubAggs);
                                                objCurStatsField.setUpper_outer_fence(objSubStats.getValue());
                                            }
                                        }
                                    }

                                    if (mapMismatchStats.containsKey(strCurFieldName)) {
                                        objCurStatsField.setMismatched(mapMismatchStats.get(strCurFieldName).get(0));
                                        objCurStatsField.setMismatched_ratio(mapMismatchStats.get(strCurFieldName).get(1));
                                    }

                                    if (mapNullStats.containsKey(strCurFieldName)) {
                                        objCurStatsField.setNullity(mapNullStats.get(strCurFieldName).get(0));
                                        objCurStatsField.setNullity_ratio(mapNullStats.get(strCurFieldName).get(1));
                                    }

                                    mapFieldStat.put(strCurFieldName, objCurStatsField);
                                }
                            }
                        }
                    }
                }

                if (lstStringField != null && lstStringField.size() > 0) {
                    for (int intCount = 0; intCount < lstStringField.size(); intCount++) {
                        String strCurField = lstStringField.get(intCount);
                        if (!mapFieldStat.containsKey(strCurField)) {
                            ESFieldStatModel objCurFieldStat = new ESFieldStatModel();
                            Boolean bCanAdd = false;

                            if (mapMismatchStats.containsKey(strCurField)) {
                                objCurFieldStat.setMismatched(mapMismatchStats.get(strCurField).get(0));
                                objCurFieldStat.setMismatched_ratio(mapMismatchStats.get(strCurField).get(1));
                                bCanAdd = true;
                            }

                            if (mapNullStats.containsKey(strCurField)) {
                                objCurFieldStat.setNullity(mapNullStats.get(strCurField).get(0));
                                objCurFieldStat.setNullity_ratio(mapNullStats.get(strCurField).get(1));
                                bCanAdd = true;
                            }

                            if (bCanAdd) {
                                mapFieldStat.put(strCurField, objCurFieldStat);
                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapFieldStat;
    }

    private void closeESClient(TransportClient objESClient) {
        // try {
        // if (objESClient != null) {
        // objESClient.close();
        // objESClient.threadPool().shutdown();
        // objESClient = null;
        // }
        // } catch (Exception objEx) {
        // objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        // }
    }

    private String generateMergingIDScript(MergingDataRequestModel objMergingRequestModel) {
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objMatrixStat;
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
                        bIsCreated = createIndex(strIndex, strType, null, null, mapFieldMapping, false);
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsCreated;
    }

    private List<String> generateDataTypeConvertScript(String strField, String strNewField, String strConvertedDataType,
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return new ArrayList<>(Arrays.asList(strConvertScript, strCatchConvertScript));
    }

    private Map<String, Map<String, ESMappingFieldModel>> createNewMappingField(String strConvertedDataType, String strNewField) {
        Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties = new HashMap<>();
        Map<String, ESMappingFieldModel> mapFieldMapping = new HashMap<>();

        ESMappingFieldModel objMappingField = createMappingField(strConvertedDataType,
                strConvertedDataType.equals(ESFilterOperationConstant.DATA_TYPE_DATE) ? true : false);
        mapFieldMapping.put(strNewField, objMappingField);
        mapFieldProperties.put("properties", mapFieldMapping);

        return mapFieldProperties;
    }

    private List<String> generateFieldPrepActionScript(ESPrepAbstractModel objPrepAction) {
        List<String> lstScript = new ArrayList<>();

        if (objPrepAction instanceof ESPrepFieldModel) {
            ESPrepFieldModel objPrep = (ESPrepFieldModel) objPrepAction;

            if (objPrep != null && objPrep.getIndex() != null && objPrep.getType() != null) {
                //Generate remove script field
                if (objPrep.getRemove_fields() != null && objPrep.getRemove_fields().size() > 0) {
                    for (String strField: objPrep.getRemove_fields()) {
                        String[] multipleFields = strField.split(",");
                        for (int i=0; i < multipleFields.length; i++) {
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

    private String generatePrepActionScript(ESPrepAbstractModel objPrepAction) {
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
                    Map<String, ESFieldStatModel> mapStat = statsField(objPrep.getIndex(), objPrep.getType(), Arrays.asList(objPrep.getSelected_field().get(0)), null, true);

                    if (mapStat != null && mapStat.containsKey(objPrep.getSelected_field().get(0))) {
                        objStatField = mapStat.get(objPrep.getSelected_field().get(0));
                    }
                }

                strScript = generateStatisticFunctionScript(objPrep, objStatField);
            }
        }

        return strScript;
    }

    private void handleSimpleSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        Long lNumOfRow = objPrep.getNum_of_rows();
        List<String> lstSelectedField = objPrep.getSelected_fields();

        if (lstSelectedField == null) {

        } else if (lstSelectedField.size() == 1) {

        } else if (lstSelectedField.size() > 1) {

        }
    }

    private void handleSystematicSamplingAction(ESPrepFunctionSamplingModel objPrep) {

    }

    private void handleDistributionSamplingAction(ESPrepFunctionSamplingModel objPrep) {

    }

    private void handleSamplingAction(ESPrepFunctionSamplingModel objPrep) {
        switch (objPrep.getSampling_op()) {
            case ESFilterOperationConstant.FUNCTION_SAMPLING_SIMPLE:
                handleSimpleSamplingAction(objPrep);
                break;
            case ESFilterOperationConstant.FUNCTION_SAMPLING_SYSTEMATIC:
                handleSystematicSamplingAction(objPrep);
                break;
            case ESFilterOperationConstant.FUNCTION_SAMPLING_DISTRIBUTION:
                handleDistributionSamplingAction(objPrep);
                break;
            case ESFilterOperationConstant.FUNCTION_SAMPLING_SMOTE:
                break;
        }
    }

    private String generateStatisticFunctionScript(ESPrepFunctionStatisticModel objPrep, ESFieldStatModel objFieldStat) {
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

    private String generateArithmeticFunctionScript(String strField, String newFieldName,
                                                    String strArithmeticOperation, String strArithmeticParam1, String strArithmeticParam2) {
        String strFormatScript = "";

        strField = ConverterUtil.convertDashField(strField);
        newFieldName = ConverterUtil.convertDashField(newFieldName);

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
        }

        return strFormatScript;
    }

    private List<String> getNewFieldFromAction(ESPrepAbstractModel objPrepAction, String strOldField, String strNewField) {
        String strNewFieldName = "";
        String strNewFieldType = "";

        if (objPrepAction instanceof ESPrepFieldModel) {
            strNewFieldName = strNewField;
            List<ESFieldModel> lstField = getFieldsMetaData(objPrepAction.getIndex(), objPrepAction.getType(),
                    new ArrayList<>(Arrays.asList(strOldField)), true);

            strNewFieldType = lstField.get(0).getType();
        }

        if (objPrepAction instanceof  ESPrepFormatModel) {
            ESPrepFormatModel objFormat = (ESPrepFormatModel)objPrepAction;
            strNewFieldName = objFormat.getNew_field_name();

            List<ESFieldModel> lstField = getFieldsMetaData(objFormat.getIndex(), objFormat.getType(),
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

        List<String> lstNewField = new ArrayList<>();

        if (strNewFieldName != null && !strNewFieldName.isEmpty() && strNewFieldType != null && !strNewFieldType.isEmpty()) {
            lstNewField.add(strNewFieldName);
            lstNewField.add(strNewFieldType);
        }

        return lstNewField;
    }

    private Boolean prepBulkAction(String strIndex, String strType, ESPrepAbstractModel objPrepAction, Integer intPageSize) {
        Boolean bIsFinish = true;

        //0. Create new field
        List<String> lstNewFieldInfo = getNewFieldFromAction(objPrepAction, "", "");

        if (lstNewFieldInfo != null && lstNewFieldInfo.size() == 2) {
            Map<String, Map<String, ESMappingFieldModel>> mapFieldProperties
                    = createNewMappingField(lstNewFieldInfo.get(1), lstNewFieldInfo.get(0));
            PutMappingResponse objPutMappingResponse = null;

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
            Long lTimeValue = 60000l;

            SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lTimeValue))
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
                        objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));

                        break;
                    }

                    objLogger.info("Cur Hit: " + lCurNumHit);
                    objLogger.info("Total Hits: " + lTotalHit);

                    //3. Continue to scroll
                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lTimeValue)).get();
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

    private List<String> getNotNullField(String strIndex, String strType, List<String> lstField) {
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

                for (String strField: lstField) {
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstNotNullField;
    }

    private String generateChangeFieldDataTypeScript(String strField,
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

    private String generateFormatDataScript(String strField, String newFieldName,
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

    private Boolean handleFields(String strIndex, String strType, ESPrepFieldModel objPrepFieldModel) {
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
                            = createNewMappingField(lstNewFieldInfo.get(1), lstNewFieldInfo.get(0));
                    PutMappingResponse objPutMappingResponse = null;

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
            Long lTimeValue = 60000l;

            SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                    .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lTimeValue))
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
                        objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));

                        break;
                    }

                    objLogger.info("Cur Hit: " + lCurNumHit);
                    objLogger.info("Total Hits: " + lTotalHit);

                    //3. Continue to scroll
                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(lTimeValue)).get();
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

    private Boolean handleDocuments(String strIndex, String strType, List<String> lstRemoveRowIdx,
                                    HashMap<String, Integer> mapCopyRowIdx) {
        Boolean bIsHandled = false;

        try {
            if (objESClient != null) {
                Boolean bIsExistIndex = false;
                Boolean bIsExistType = false;

                // Check Index and Type is existed
                List<ESIndexModel> lstIndices = getAllIndices();

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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsHandled;
    }

    private ESMappingFieldModel createMappingField(String strFieldType, Boolean bIsDateField) {
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

    private String getLatestIndexName(HashMap<String, String> mapIndexMapping, String strOldIndex) {
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

        List<Boolean> lstReturn = new ArrayList<>();
        lstReturn.add(bIsExistsIndex);
        lstReturn.add(bIsExistsType);

        return lstReturn;
    }

    public Boolean deleteIndex(String strIndex) {
        Boolean bIsDeleted = false;

        try {
            if (objESClient != null) {
                DeleteIndexResponse objDeleteResponse = objESClient.admin().indices().prepareDelete(strIndex).get();

                if (objDeleteResponse != null && objDeleteResponse.isAcknowledged()) {
                    bIsDeleted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsDeleted;
    }

    private SearchResponse searchESWithPaging(String strIndex, String strType, Integer intFromDocIdx,
                                              Integer intPageSize) {
        try {
            if (objESClient != null) {
                SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSize(intPageSize).setFrom(intFromDocIdx);

                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(intPageSize).from(intFromDocIdx)
                        .sort("_doc");

                objRequestBuilder.setSource(objSearchSourceBuilder);
                SearchResponse objSearchResponse = objRequestBuilder.get();

                return objSearchResponse;
            } else {
                return null;
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));

            return null;
        }
    }

    private SearchResponse searchESWithScan(String strIndex, String strType, Integer intPageSize) {
        SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(60000))
                .setSize(intPageSize).get();

        do {
            if (objSearchResponse != null && objSearchResponse.getHits() != null
                    && objSearchResponse.getHits().getTotalHits() > 0
                    && objSearchResponse.getHits().getHits() != null
                    && objSearchResponse.getHits().getHits().length > 0) {
            }

            objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                    .setScroll(new TimeValue(60000)).get();
        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                && objSearchResponse.getHits().getHits() != null
                && objSearchResponse.getHits().getHits().length > 0);

        return objSearchResponse;
    }

    @SuppressWarnings("unchecked")
    private Boolean writeESDataToCSVFile(FileWriter objFileWriter, SearchResponse objSearchResponse,
                                         Boolean bIsIncludeHeader) {
        Boolean bIsWrote = true;

        try {
            Integer intCount = 0;

            for (SearchHit objCurHit : objSearchResponse.getHits().getHits()) {
                String strSource = objCurHit.getSourceAsString();

                if (strSource != null && !strSource.isEmpty()) {
                    HashMap<String, Object> mapSource = objMapper.readValue(strSource, HashMap.class);

                    if (mapSource != null && mapSource.size() > 0) {
                        if (intCount == 0 && bIsIncludeHeader) {
                            List<Object> lstHeader = Arrays
                                    .asList(mapSource.keySet().toArray(new Object[mapSource.keySet().size()]));

                            lstHeader = lstHeader.stream().map(str -> str.toString().replace("-", ".")).collect(Collectors.toList());
                            CSVUtil.writeLine(objFileWriter, lstHeader);

                            objLogger.info("INFO: " + Arrays.toString(lstHeader.toArray()));
                        }

                        List<Object> lstValue = Arrays
                                .asList(mapSource.values().toArray(new Object[mapSource.values().size()]));
                        CSVUtil.writeLine(objFileWriter, lstValue);
                    }
                }

                intCount++;
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
            bIsWrote = false;
        }

        return bIsWrote;
    }

    @SuppressWarnings("unchecked")
    public Boolean createIndex(String strIndex, String strType, List<?> lstData, String strDateField,
                               HashMap<String, ESMappingFieldModel> mapMappingField, Boolean bDelIndexIfExisted) {
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
                        Long lTotalData = (long)lstData.size();
                        Long lCurData = 0l;
                        for (int intCount = 0; intCount < lstData.size(); intCount++) {
                            HashMap<String, Object> mapCur = (HashMap<String, Object>)lstData.get(0);

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
                                Object objValue = ConverterUtil.convertStringToDataType(curItem.getValue().toString());
                                objLogger.info("objValue: " + objValue);

                                strFieldType = objValue.getClass().getCanonicalName().toLowerCase();
                            } else {
                                strFieldType = classZ.getDeclaredField(curItem.getKey()).getType().getTypeName()
                                        .toLowerCase();
                            }
                            objLogger.info("FieldType: " + curItem.getKey() + " - " + strFieldType);

                            ESMappingFieldModel objMappingField = new ESMappingFieldModel();
                            objMappingField.setType(null);
                            objMappingField.setFielddata(null);
                            objMappingField.setCopy_to(null);
                            objMappingField.setIndex(null);

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
                                Long lTotalNA = lstData.stream().map(objItem -> ((HashMap<String,Object>)objItem).get(curItem.getKey()))
                                        .filter(item -> JacksonFilter.checkNAString(item.toString())).count();

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

                            objCreateIndexResponse = objESClient.admin().indices().prepareCreate(strIndex)
                                    .setSettings(Settings.builder()
                                            .put("index.mapping.total_fields.limit", mapMappingField.size() * 10)
                                            .put("index.max_result_window", 1000000000))
                                    .get();

                            objLogger.info("objCreateIndexResponse: " + objCreateIndexResponse);
                        }

                        if (bIsExistsIndex
                                || (objCreateIndexResponse != null && objCreateIndexResponse.isAcknowledged())) {
                            PutMappingResponse objPutMappingResponse = objESClient.admin().indices()
                                    .preparePutMapping(strIndex).setType(strType)
                                    .setSource(strJSONMappingData, XContentType.JSON).get();

                            if (objPutMappingResponse != null && objPutMappingResponse.isAcknowledged()) {
                                try {
                                    HashMap<String, Object> mapSettings = new HashMap<>();
                                    mapSettings.put("script.max_compilations_per_minute", 1000000);
                                    //ClusterUpdateSettingsRequestBuilder objBuilder = Settings.builder().put("script.max_compilations_per_minute", 1000000);
                                    //objESClient.admin().indices().prepareUpdateSettings().setIndex(strIndex).setType(strType).setDoc("{\"transient.script.max_compilations_per_minute\" : 1000000}", XContentType.JSON).get();
                                    //objESClient.admin().cluster().prepareUpdateSettings().setTransientSettings(mapSettings).get();
                                    //objESClient.admin().indices().prepareUpdateSettings(strIndex).setSettings(mapSettings).get();
                                } catch (Exception objEx) {
                                    objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
                                }

                                bIsCreated = true;
                            }
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsCreated;
    }

    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel) {
        Boolean bIsInserted = false;

        try {
            if (lstFieldModel == null) {
                createIndex(strIndex, strType, lstData, strFieldDate, null, false);
                lstFieldModel = getFieldsMetaData(strIndex, strType, null, false);
            }

            if (objESClient != null) {
                ObjectMapper objCurrentMapper = new ObjectMapper();
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objCurrentMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

                BulkProcessor objBulkProcessor = createBulkProcessor(objESClient, lstData.size());

                if (objBulkProcessor != null) {
                    for (int intCount = 0; intCount < lstData.size(); intCount++) {
                        Object objData = lstData.get(intCount);

                        if (objData instanceof HashMap) {
                            HashMap<String, Object> mapOriginal = (HashMap<String, Object>)objData;

                            mapOriginal = ConverterUtil.convertMapToMapType(mapOriginal, lstFieldModel);

                            objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strIndex + "_" + strType + "_" + intCount)
                                    .source(objCurrentMapper.writeValueAsString(mapOriginal), XContentType.JSON));
//
//                            if (mapOriginal.entrySet().stream().filter(item -> item.getKey().contains(".")).count() > 0) {
//                                HashMap<String, Object> mapNew = new HashMap<>();
//
//                                for (Map.Entry<String, Object> item : mapOriginal.entrySet()) {
//                                    mapNew.put(item.getKey().replace(".", "-"), item.getValue());
//                                }
//
//                                objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strIndex + "_" + strType + "_" + intCount)
//                                        .source(objCurrentMapper.writeValueAsString(mapNew), XContentType.JSON));
//                            } else {
//                                objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strIndex + "_" + strType + "_" + intCount)
//                                        .source(objCurrentMapper.writeValueAsString(lstData.get(intCount)), XContentType.JSON));
//                            }
                        } else {
                            objBulkProcessor.add(new IndexRequest(strIndex, strType).id(strIndex + "_" + strType + "_" + intCount)
                                    .source(objCurrentMapper.writeValueAsString(lstData.get(intCount)), XContentType.JSON));
                        }
                    }

                    objBulkProcessor.flush();
                    objBulkProcessor.awaitClose(10, TimeUnit.MINUTES);

                    bIsInserted = true;
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsInserted;
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
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
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lstReturnField;
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

    public Map<String, List<ESFilterOperationModel>> getMatrixFilterOperation() {
        Map<String, List<ESFilterOperationModel>> mapResult = new HashMap<>();

        ESFilterTextDefaultModel objDefaultText = new ESFilterTextDefaultModel();
        objDefaultText.setInput_idx(1);
        objDefaultText.setPlace_holder("Enter value...");

        ESFilterCustomModel objSingleValueCustomModel = new ESFilterCustomModel();
        objSingleValueCustomModel.setNum_of_input(1);
        objSingleValueCustomModel.setIs_single_value(true);
        objSingleValueCustomModel.setUi(new ArrayList<>(Arrays.asList(objDefaultText)));

        List<ESFilterOperationModel> lstNumberOpPredefinedFilter = new ArrayList<>();
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.CORRELATION, "CORRELATION",
                new ArrayList<>(), objSingleValueCustomModel));

        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.COVARIANCE, "COVARIANCE",
                new ArrayList<>(), objSingleValueCustomModel));

        mapResult.put("integer", lstNumberOpPredefinedFilter);
        mapResult.put("long", lstNumberOpPredefinedFilter);
        mapResult.put("double", lstNumberOpPredefinedFilter);
        mapResult.put("byte", lstNumberOpPredefinedFilter);
        mapResult.put("float", lstNumberOpPredefinedFilter);
        mapResult.put("short", lstNumberOpPredefinedFilter);

        return mapResult;
    }

    public Map<String, List<ESFilterOperationModel>> getFilterOperation() {
        Map<String, List<ESFilterOperationModel>> mapResult = new HashMap<>();
        List<ESFilterOperationModel> lstNumberOpPredefinedFilter = new ArrayList<>();
        List<ESFilterOperationModel> lstTextOpPredefinedFilter = new ArrayList<>();
        List<ESFilterOperationModel> lstDateOpPredefinedFilter = new ArrayList<>();
        List<ESFilterOperationModel> lstBoolOpPredefinedFilter = new ArrayList<>();

        List<String> lstWithPredefined = new ArrayList<>();
        List<String> lstWithBoolean = new ArrayList<>();

        lstWithPredefined.add(ESFilterOperationConstant.FILTER_OUTLIER_EXTREME);
        lstWithPredefined.add(ESFilterOperationConstant.FILTER_OUTLIER_MILD);
        lstWithPredefined.add(ESFilterOperationConstant.FILTER_LCL);
        lstWithPredefined.add(ESFilterOperationConstant.FILTER_UCL);
        lstWithPredefined.add(ESFilterOperationConstant.FILTER_LCL_UCL);

        ESFilterTextDefaultModel objFromTextDefault = new ESFilterTextDefaultModel();
        objFromTextDefault.setInput_idx(1);
        objFromTextDefault.setPlace_holder("From");

        ESFilterTextDefaultModel objToTextDefault = new ESFilterTextDefaultModel();
        objToTextDefault.setInput_idx(2);
        objToTextDefault.setPlace_holder("To");

        ESFilterTextDefaultModel objDefaultText = new ESFilterTextDefaultModel();
        objDefaultText.setInput_idx(1);
        objDefaultText.setPlace_holder("Enter value...");

        ESFilterCustomModel objBetweenCustomModel = new ESFilterCustomModel();
        objBetweenCustomModel.setNum_of_input(2);
        objBetweenCustomModel.setIs_single_value(true);
        objBetweenCustomModel.setUi(new ArrayList<>(Arrays.asList(objFromTextDefault, objToTextDefault)));

        ESFilterCustomModel objSingleValueCustomModel = new ESFilterCustomModel();
        objSingleValueCustomModel.setNum_of_input(1);
        objSingleValueCustomModel.setIs_single_value(true);
        objSingleValueCustomModel.setUi(new ArrayList<>(Arrays.asList(objDefaultText)));

        ESFilterCustomModel objMultipleValueCustomModel = new ESFilterCustomModel();
        objMultipleValueCustomModel.setNum_of_input(1);
        objMultipleValueCustomModel.setIs_single_value(false);
        objMultipleValueCustomModel.setUi(new ArrayList<>(Arrays.asList(objDefaultText)));

        lstWithBoolean.add("true");
        lstWithBoolean.add("false");

        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS, "IS",
                lstWithPredefined, objSingleValueCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT, "IS NOT",
                lstWithPredefined, objSingleValueCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_BETWEEN, "BETWEEN",
                lstWithPredefined, objBetweenCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_BETWEEN,
                "NOT BETWEEN", lstWithPredefined, objBetweenCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_ONE_OF, "IS ONE OF",
                new ArrayList<>(), objMultipleValueCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_ONE_OF,
                "IS NOT ONE OF", new ArrayList<>(), objMultipleValueCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.EXISTS, "EXISTS",
                new ArrayList<>(), objMultipleValueCustomModel));
        lstNumberOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.DOES_NOT_EXIST,
                "NOT EXIST", new ArrayList<>(), objMultipleValueCustomModel));

        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS, "IS", new ArrayList<>(),
                objSingleValueCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT, "IS NOT",
                new ArrayList<>(), objSingleValueCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_BETWEEN, "BETWEEN",
                new ArrayList<>(), objBetweenCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_BETWEEN,
                "NOT BETWEEN", new ArrayList<>(), objBetweenCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_ONE_OF, "IS ONE OF",
                new ArrayList<>(), objMultipleValueCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_ONE_OF,
                "IS NOT ONE OF", new ArrayList<>(), objMultipleValueCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.EXISTS, "EXISTS",
                new ArrayList<>(), objMultipleValueCustomModel));
        lstTextOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.DOES_NOT_EXIST, "NOT EXIST",
                new ArrayList<>(), objMultipleValueCustomModel));

        lstDateOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_BETWEEN, "BETWEEN",
                new ArrayList<>(), objBetweenCustomModel));
        lstDateOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_BETWEEN,
                "NOT BETWEEN", new ArrayList<>(), objBetweenCustomModel));

        lstBoolOpPredefinedFilter
                .add(new ESFilterOperationModel(ESFilterOperationConstant.IS, "IS", lstWithBoolean, null));
        lstBoolOpPredefinedFilter
                .add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT, "IS NOT", lstWithBoolean, null));
        lstBoolOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_ONE_OF, "IS ONE OF",
                lstWithBoolean, objMultipleValueCustomModel));
        lstBoolOpPredefinedFilter.add(new ESFilterOperationModel(ESFilterOperationConstant.IS_NOT_ONE_OF,
                "IS NOT ONE OF", lstWithBoolean, objMultipleValueCustomModel));

        mapResult.put("keyword", lstTextOpPredefinedFilter);
        mapResult.put("integer", lstNumberOpPredefinedFilter);
        mapResult.put("long", lstNumberOpPredefinedFilter);
        mapResult.put("double", lstNumberOpPredefinedFilter);
        mapResult.put("byte", lstNumberOpPredefinedFilter);
        mapResult.put("float", lstNumberOpPredefinedFilter);
        mapResult.put("short", lstNumberOpPredefinedFilter);
        mapResult.put("date", lstDateOpPredefinedFilter);
        mapResult.put("boolean", lstBoolOpPredefinedFilter);

        return mapResult;
    }

    @SuppressWarnings("unchecked")
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(String strIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField, Integer intFromRow, Integer intNumRow, Integer intFromField,
                                                                   Integer intNumField, Boolean bIsSimpleStats, ESFilterAllRequestModel objFilterAllRequest) {
        HashMap<String, Object> mapResult = new HashMap<>();
        List<HashMap<String, Object>> lstData = new ArrayList<HashMap<String, Object>>();
        Long lTotalResult = 0L;

        try {
            lstSelectedField = (objFilterAllRequest != null && objFilterAllRequest.getSelected_fields() != null
                    && objFilterAllRequest.getSelected_fields().size() > 0) ? objFilterAllRequest.getSelected_fields()
                    : lstSelectedField;

            ESQueryResultModel objQueryResponseData = getResponseDataFromQueryByFieldIdxAndRowIdx(strIndex, strType,
                    objFilterAllRequest, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField,
                    bIsSimpleStats);

            if (objQueryResponseData != null && objQueryResponseData.getSearch_response() != null) {
                SearchResponse objResponseData = objQueryResponseData.getSearch_response();

                if (objResponseData != null && objResponseData.getHits() != null) {
                    lTotalResult = objResponseData.getHits().totalHits;

                    if (lTotalResult > 0) {
                        for (int intCount = 0; intCount < objResponseData.getHits().getHits().length; intCount++) {
                            String strColId = objResponseData.getHits().getHits()[intCount].getId();

                            String strHitJSON = objResponseData.getHits().getHits()[intCount].getSourceAsString();

                            if (strHitJSON != null && !strHitJSON.isEmpty()) {
                                HashMap<String, Object> objCurData = objMapper.readValue(strHitJSON, HashMap.class);

                                if (objCurData != null) {
                                    objCurData.put("_id", strColId);
                                    lstData.add(objCurData);
                                }
                            }
                        }

                        mapResult.put("total_data", lTotalResult);
                        mapResult.put("data", lstData);
                        mapResult.put("agg_data", objQueryResponseData.getAgg_fields());
                        mapResult.put("total_returned_rows", lstData.size());
                        mapResult.put("total_index_fields", objQueryResponseData.getTotal_fields());
                        mapResult.put("total_returned_fields", objQueryResponseData.getNum_selected_fields());
                        mapResult.put("fields", objQueryResponseData.getSelected_fields());
                        mapResult.put("next_row_idx", lstData.size() > 0 ? (intFromRow + lstData.size()) : -1);
                        mapResult.put("next_field_idx",
                                (objQueryResponseData.getNum_selected_fields() > 0 && (intFromField
                                        + objQueryResponseData.getNum_selected_fields()) <= objQueryResponseData
                                        .getTotal_fields())
                                        ? (intFromField + objQueryResponseData.getNum_selected_fields()
                                        - 1)
                                        : -1);
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapResult;
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
                    objLogger.error("INFO: " + objResponse.toString());
                    bIsMerged = true;
                }
            } catch (Exception objEx) {
                objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
            }
        }

        return bIsMerged;
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

                SearchResponse objSearchResponse = searchESWithPaging(strIndex, strType, 0, intPageSize);

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
                                SearchResponse objNextSearchResponse = searchESWithPaging(strIndex, strType,
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        if (bIsExported) {
            return strFileName;
        } else {
            return "";
        }
    }

    private void refreshIndex(String strIndex) {
        try {
            if (objESClient != null) {
                objESClient.admin().indices().refresh(new RefreshRequest(strIndex)).get();
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }
    }

    public String exportESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize) {
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

                //Refresh index before export
                refreshIndex(strIndex);

                SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(60000))
                        .setSize(intPageSize).get();

                do {
                    if (objSearchResponse != null && objSearchResponse.getHits() != null
                            && objSearchResponse.getHits().getTotalHits() > 0
                            && objSearchResponse.getHits().getHits() != null
                            && objSearchResponse.getHits().getHits().length > 0) {
                        Boolean bIsWriteCSV = writeESDataToCSVFile(objFileWriter, objSearchResponse, true);

                        if (!bIsWriteCSV) {
                            bIsExported = false;
                            break;
                        }
                    }

                    objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                            .setScroll(new TimeValue(60000)).get();
                } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                        && objSearchResponse.getHits().getHits() != null
                        && objSearchResponse.getHits().getHits().length > 0);

                objFileWriter.flush();
                objFileWriter.close();
            }
        } catch (Exception objEx) {
            bIsExported = false;
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        if (bIsExported) {
            return strFileName;
        } else {
            return "";
        }
    }

    public Boolean prepESData(List<ESPrepAbstractModel> lstPrepOp) {
        Boolean bIsPrepAll = false;

        try {
            HashMap<String, String> mapIndexMapping = new HashMap<>();

            if (objESClient != null && lstPrepOp != null && lstPrepOp.size() > 0) {
                for (int intCount = 0; intCount < lstPrepOp.size(); intCount++) {
                    ESPrepAbstractModel objPrepOp = lstPrepOp.get(intCount);
                    String strCurIndex = getLatestIndexName(mapIndexMapping, objPrepOp.getIndex());

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

                    if ((objPrepOp instanceof  ESPrepFormatModel)
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
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return bIsPrepAll;
    }

    public Boolean deleteField(String strIndex, String strType, String strField) {
        String strRemoveScript = "ctx._source.remove(\"" + strField + "\")";
//        UpdateByQueryRequestBuilder objUpdateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(objESClient);
//        objUpdateByQuery.source(strIndex).abortOnVersionConflict(false)
//                .script(new Script(ScriptType.INLINE, "painless", strRemoveScript,
//                        Collections.emptyMap()));
//
//        BulkByScrollResponse objRespone = objUpdateByQuery.get(TimeValue.timeValueMinutes(10));
//        if (objRespone != null) {
//            return true;
//        }
//        return false;
//
        Long lTimeValue = 60000l;
        SearchResponse objSearchResponse = objESClient.prepareSearch(strIndex).setTypes(strType)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(lTimeValue))
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
                    objLogger.error("Cannot delete field ({}) of index ({}). Error: {}", strField, strIndex, e.getMessage());
                    return false;
                }

                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                        .setScroll(new TimeValue(lTimeValue)).get();
            } else {
                break;
            }
        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                && objSearchResponse.getHits().getHits() != null
                && objSearchResponse.getHits().getHits().length > 0);

        return true;
    }
}