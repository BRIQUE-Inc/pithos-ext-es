package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.chronotics.pithos.ext.es.model.*;
import org.chronotics.pithos.ext.es.util.ESFilterConverterUtil;
import org.chronotics.pithos.ext.es.util.ESFilterOperationConstant;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticFilter {
    protected Logger objLogger = LoggerFactory.getLogger(ElasticFilter.class);
    ElasticConnection objESConnection;
    TransportClient objESClient;
    ObjectMapper objMapper = new ObjectMapper();

    public ElasticFilter(ElasticConnection objESConnection) {
        this.objESConnection = objESConnection;
        this.objESClient = this.objESConnection.objESClient;
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
                                                                   List<String> lstSelectedField,
                                                                   Integer intFromRow, Integer intNumRow,
                                                                   Integer intFromField, Integer intNumField,
                                                                   Boolean bIsSimpleStats,
                                                                   ESFilterAllRequestModel objFilterAllRequest,
                                                                   List<ESSortingField> lstSortingField) {
        HashMap<String, Object> mapResult = new HashMap<>();
        List<HashMap<String, Object>> lstData = new ArrayList<HashMap<String, Object>>();
        Long lTotalResult = 0L;

        try {
            lstSelectedField = (objFilterAllRequest != null && objFilterAllRequest.getSelected_fields() != null
                    && objFilterAllRequest.getSelected_fields().size() > 0) ? objFilterAllRequest.getSelected_fields()
                    : lstSelectedField;

            ESQueryResultModel objQueryResponseData = getResponseDataFromQueryByFieldIdxAndRowIdx(strIndex, strType,
                    objFilterAllRequest, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField,
                    bIsSimpleStats, lstSortingField);

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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapResult;
    }

    protected ESQueryResultModel getResponseDataFromQueryByFieldIdxAndRowIdx(String strIndex, String strType,
                                                                             ESFilterAllRequestModel objFilterAllRequest, List<String> lstSelectedField,
                                                                             Integer intFromRow, Integer intNumRow,
                                                                             Integer intFromCol, Integer intNumCol,
                                                                             Boolean bIsSimpleStats, List<ESSortingField> lstSortingField) {
        ESQueryResultModel objQueryResult = new ESQueryResultModel();
        SearchResponse objSearchResponse = new SearchResponse();

        try {
            Map<String, Map<String, List<ESFieldModel>>> mapFieldOfIndex = objESConnection.getFieldsOfIndices(Arrays.asList(strIndex),
                    Arrays.asList(strType), null, true);

            if ((mapFieldOfIndex != null && mapFieldOfIndex.size() > 0 && mapFieldOfIndex.containsKey(strIndex))
                || (strIndex.contains("*"))){
                List<ESFieldModel> lstFieldModel = new ArrayList<>();

                if (strIndex.contains("*")) {
                    String strIndexPattern = strIndex.replace("*", "");

                    for (Map.Entry<String, Map<String, List<ESFieldModel>>> curEntry : mapFieldOfIndex.entrySet()) {
                        if (curEntry.getKey().contains(strIndexPattern)) {
                            lstFieldModel = curEntry.getValue().get(strType);
                            break;
                        }
                    }
                } else {
                    if (mapFieldOfIndex.get(strIndex) != null && mapFieldOfIndex.get(strIndex).size() > 0
                            && mapFieldOfIndex.get(strIndex).containsKey(strType)) {
                        lstFieldModel = mapFieldOfIndex.get(strIndex).get(strType);
                    }
                }

                if (lstFieldModel != null && lstFieldModel.size() > 0) {
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
                                    lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, objFilterAllRequest.getDeleted_rows(), lstSortingField);
                        } else {
                            objSearchResponse = getResponseDataFromQuery(new String[]{strIndex},
                                    new String[]{strType}, lstSourceField.toArray(new String[lstSourceField.size()]),
                                    lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, new ArrayList<>(), lstSortingField);
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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objQueryResult;
    }

    protected BoolQueryBuilder generateAggQueryBuilder(String strIndex, String strType, BoolQueryBuilder objQueryBuilder,
                                                     List<ESFilterRequestModel> lstNotAddedFilterRequest, List<ESFieldModel> lstFieldModel) {
        List<String> lstNotAddedFieldName = lstNotAddedFilterRequest.stream()
                .filter(objFilter -> (objFilter.getFiltered_conditions() != null
                        && objFilter.getFiltered_conditions().size() > 0))
                .map(objFiltered -> objFiltered.getFiltered_on_field()).collect(Collectors.toList());

        List<String> lstNumericField = new ArrayList<>();
        List<String> lstTextField = new ArrayList<>();

        lstNumericField = lstFieldModel.stream()
                .filter(objField -> !objField.getType().equals("text") && !objField.getType().equals("keyword") && !objField.getType().equals("date") && lstNotAddedFieldName.contains(objField.getFull_name()))
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
    protected SearchResponse getResponseDataFromQuery(String[] arrIndex, String[] arrType, String[] arrSource,
                                                      List<ESFilterRequestModel> lstFilterRequest, Boolean bIsReversedFilter,
                                                      Integer intFrom, Integer intSize,
                                                      List<ESFieldModel> lstFieldModel, List<String> lstDeletedRows,
                                                      List<ESSortingField> lstSortingField) {
        SearchResponse objSearchResponse = new SearchResponse();

        try {
            SearchRequestBuilder objRequestBuilder = objESClient.prepareSearch(arrIndex).setTypes(arrType)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);

            SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
            objSearchSourceBuilder.size(intSize).from(intFrom);

            if (lstSortingField == null || lstSortingField.size() <= 0) {
                objSearchSourceBuilder.sort("_doc");
            } else {
                for (int intCount = 0; intCount < lstSortingField.size(); intCount++) {
                    ESSortingField objSortingField = lstSortingField.get(intCount);
                    SortOrder objSortOrder = SortOrder.ASC;

                    if (objSortingField.getOrder_by().equals(2)) {
                        objSortOrder = SortOrder.DESC;
                    }

                    SortBuilder objSortBuilder = SortBuilders.fieldSort(objSortingField.getField_name()).order(objSortOrder);
                    objSearchSourceBuilder.sort(objSortBuilder);
                }
            }

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
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return objSearchResponse;
    }

    protected HashMap<String, List<Double>> statsNullityOfField(String strIndex, String strType, List<String> lstField) {
        HashMap<String, List<Double>> mapNullity = new HashMap<>();

        try {
            if (objESClient != null) {
                Long lTotalHit = getTotalHit(strIndex, strType);

                //Get Total Hit First
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);

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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapNullity;
    }

    // Way to generate Histogram of array data:
    // http://www.oswego.edu/~srp/stats/hist_con.htm
    protected List<ESFieldAggModel> getHistogramOfField(String strIndex, String strType, List<String> lstField,
                                                      Boolean bIsSimpleStats) {
        List<ESFieldAggModel> lstAggResult = new ArrayList<>();
        Map<String, List<ESFieldPointModel>> mapHistogramPoint = new HashMap<>();
        Map<String, ESFieldStatModel> mapFieldStats = new HashMap<>();

        try {
            // Get data type of fields
            List<ESFieldModel> lstFieldMeta = objESConnection.getFieldsMetaData(strIndex, strType, lstField, true);

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
                            .addAggregation(AggregationBuilders.min(strMinName).field(lstNumberField.get(intCount).trim()));
                    objSearchRequestBuilder
                            .addAggregation(AggregationBuilders.max(strMaxName).field(lstNumberField.get(intCount).trim()));
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
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
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

    protected Long getTotalHit(String strIndex, String strType) {
        Long lTotalHit = 0L;

        try {
            if (objESClient != null) {
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
            }
        } catch (Exception objEx) {
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return lTotalHit;
    }

    protected Map<String, List<Double>> statsMismatchOfField(String strIndex, String strType, List<String> lstStringField) {
        Map<String, List<Double>> mapStats = new HashMap<>();

        try {
            if (objESClient != null && lstStringField != null && lstStringField.size() > 0) {
                Long lTotalHit = getTotalHit(strIndex, strType);

                //Check nullity
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);

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

                            SearchResponse objSearchResponse = objSearchRequestBuilder.setQuery(objBooleanQuery).get();

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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapStats;
    }

    protected SearchResponse searchESWithPaging(String strIndex, String strType, Integer intFromDocIdx,
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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));

            return null;
        }
    }

    protected SearchResponse searchESWithScan(String strIndex, String strType, Integer intPageSize) {
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

    // Way to calculate Outlier:
    // https://www.itl.nist.gov/div898/handbook/prc/section1/prc16.htm
    protected Map<String, ESFieldStatModel> statsField(String strIndex, String strType, List<String> lstNumberField, List<String> lstStringField,
                                                     Boolean bIsSimpleStats) {
        Map<String, ESFieldStatModel> mapFieldStat = new HashMap<>();

        try {
            if (objESClient != null) {
                Map<String, List<Double>> mapMismatchStats = statsMismatchOfField(strIndex, strType, lstStringField);

                List<String> lstCombineField = new ArrayList<>();

                if (lstNumberField != null && lstNumberField.size() > 0) {
                    lstCombineField.addAll(lstNumberField);
                }

                if (lstStringField != null && lstStringField.size() > 0) {
                    lstCombineField.addAll(lstStringField);
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
            objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return mapFieldStat;
    }

    public SearchResponse getCustomAggregationValue(String strIndex, String strType, QueryBuilder objCustomQueryBuilder, AggregationBuilder objCustomAggregationBuilder) {
        if (objESClient != null && objCustomAggregationBuilder != null) {
            try {
                SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(strIndex).setTypes(strType);
                SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                objSearchSourceBuilder.size(0);
                objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                if (objCustomQueryBuilder != null) {
                    objSearchSourceBuilder.query(objCustomQueryBuilder);
                }

                objSearchRequestBuilder.addAggregation(objCustomAggregationBuilder);

                return objSearchRequestBuilder.get(new TimeValue(10, TimeUnit.MINUTES));
            } catch (Exception objEx) {
                objLogger.warn("ERR: " + ExceptionUtil.getStrackTrace(objEx));
            }
        }

        return null;
    }
}
