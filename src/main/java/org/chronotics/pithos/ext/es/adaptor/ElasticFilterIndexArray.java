package org.chronotics.pithos.ext.es.adaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.chronotics.pithos.ext.es.model.*;
import org.chronotics.pithos.ext.es.util.ESConverterUtil;
import org.chronotics.pithos.ext.es.util.ESFilterConverterUtil;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticFilterIndexArray {
    protected Logger objLogger = LoggerFactory.getLogger(ElasticFilter.class);
    ElasticConnection objESConnection;
    TransportClient objESClient;
    ObjectMapper objMapper = new ObjectMapper();

    Long lScrollTTL = 600000L;

    public ElasticFilterIndexArray(ElasticConnection objESConnection) {
        this.objESConnection = objESConnection;
        this.objESClient = this.objESConnection.objESClient;
    }

    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(List<String> arrIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField,
                                                                   Integer intFromRow, Integer intNumRow,
                                                                   Integer intFromField, Integer intNumField,
                                                                   Integer intStatsType,
                                                                   ESFilterAllRequestModel objFilterAllRequest,
                                                                   List<ESSortingField> lstSortingField) {
        return searchDataWithFieldIdxAndRowIdx(arrIndex, strType, strQuery, lstSelectedField,
                intFromRow, intNumRow, intFromField, intNumField, intStatsType,
                objFilterAllRequest, lstSortingField, false);
    }

    public SearchResponse getCustomAggregationValue(List<String> lstIndex, String strType, QueryBuilder objCustomQueryBuilder, List<AggregationBuilder> lstCustomAggregationBuilder) {
        if (objESClient != null && lstCustomAggregationBuilder != null && lstCustomAggregationBuilder.size() > 0) {
            try {
                List<String> lstExistedIndex = objESConnection.checkIndexArrayExisted(lstIndex);

                if (lstExistedIndex != null && lstExistedIndex.size() > 0) {
                    objESConnection.refreshIndexArray(lstExistedIndex);
                    String[] arrIndex = lstExistedIndex.toArray(new String[lstExistedIndex.size()]);

                    SearchRequestBuilder objSearchRequestBuilder = objESClient.prepareSearch(arrIndex).setTypes(strType);
                    SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                    objSearchSourceBuilder.size(0);
                    objSearchRequestBuilder.setSource(objSearchSourceBuilder);

                    if (objCustomQueryBuilder != null) {
                        objSearchSourceBuilder.query(objCustomQueryBuilder);
                    }

                    for (int intCount = 0; intCount < lstCustomAggregationBuilder.size(); intCount++) {
                        objSearchRequestBuilder.addAggregation(lstCustomAggregationBuilder.get(intCount));
                    }

                    return objSearchRequestBuilder.get(new TimeValue(10, TimeUnit.MINUTES));
                }
            } catch (Exception objEx) {
                objLogger.debug(ExceptionUtil.getStackTrace(objEx));
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(List<String> lstIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField,
                                                                   Integer intFromRow, Integer intNumRow,
                                                                   Integer intFromField, Integer intNumField,
                                                                   Integer intStatsType,
                                                                   ESFilterAllRequestModel objFilterAllRequest,
                                                                   List<ESSortingField> lstSortingField, Boolean bIsRefresh) {
        HashMap<String, Object> mapResult = new HashMap<>();
        List<HashMap<String, Object>> lstData = new ArrayList<HashMap<String, Object>>();
        Long lTotalResult = 0L;

        try {
            lstSelectedField = (objFilterAllRequest != null && objFilterAllRequest.getSelected_fields() != null
                    && objFilterAllRequest.getSelected_fields().size() > 0) ? objFilterAllRequest.getSelected_fields()
                    : lstSelectedField;

            ESQueryResultModel objQueryResponseData = getResponseDataFromQueryByFieldIdxAndRowIdx(lstIndex, strType,
                    objFilterAllRequest, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField,
                    intStatsType, lstSortingField, bIsRefresh);

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
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return mapResult;
    }

    protected ESQueryResultModel getResponseDataFromQueryByFieldIdxAndRowIdx(List<String> lstIndex, String strType,
                                                                             ESFilterAllRequestModel objFilterAllRequest, List<String> lstSelectedField,
                                                                             Integer intFromRow, Integer intNumRow,
                                                                             Integer intFromCol, Integer intNumCol,
                                                                             Integer intStatsType, List<ESSortingField> lstSortingField, Boolean bIsRefresh) {
        ESQueryResultModel objQueryResult = new ESQueryResultModel();
        SearchResponse objSearchResponse = new SearchResponse();

        try {
            List<String> lstExistedIndex = objESConnection.checkIndexArrayExisted(lstIndex);

            if (lstExistedIndex != null && lstExistedIndex.size() > 0) {
                if (bIsRefresh) {
                    objESConnection.refreshIndexArray(lstExistedIndex);
                }

                Map<String, Map<String, List<ESFieldModel>>> mapFieldOfIndex = objESConnection.getFieldsOfIndices(lstExistedIndex,
                        Arrays.asList(strType), null, false);

                if (mapFieldOfIndex != null && mapFieldOfIndex.size() > 0) {
                    List<ESFieldModel> lstFieldModel = new ArrayList<>();

                    for (int intCount = 0; intCount < lstExistedIndex.size(); intCount++) {
                        String strIndex = lstExistedIndex.get(intCount);

                        if (mapFieldOfIndex.get(strIndex) != null && mapFieldOfIndex.get(strIndex).size() > 0
                                && mapFieldOfIndex.get(strIndex).containsKey(strType)) {
                            lstFieldModel.addAll(mapFieldOfIndex.get(strIndex).get(strType));
                        }
                    }

                    if (lstFieldModel != null && lstFieldModel.size() > 0) {
                        if (lstFieldModel != null && lstFieldModel.size() > 0) {
                            lstFieldModel = lstFieldModel.stream().filter(ESConverterUtil.distinctByKey(ESFieldModel::getFull_name)).distinct().collect(Collectors.toList());
                        }

                        String[] arrIndex = lstExistedIndex.toArray(new String[lstExistedIndex.size()]);

                        List<String> lstSourceField = new ArrayList<>();
                        List<ESFilterRequestModel> lstFilters = (objFilterAllRequest != null
                                && objFilterAllRequest.getFilters() != null && objFilterAllRequest.getFilters().size() > 0)
                                ? objFilterAllRequest.getFilters()
                                : new ArrayList<ESFilterRequestModel>();

                        Boolean bIsReversedFilter = (objFilterAllRequest != null && objFilterAllRequest.getIs_reversed() != null) ? objFilterAllRequest.getIs_reversed() : false;

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
                                objSearchResponse = getResponseDataFromQuery(arrIndex,
                                        new String[]{strType}, lstSourceField.toArray(new String[lstSourceField.size()]),
                                        lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, objFilterAllRequest.getDeleted_rows(), lstSortingField);
                            } else {
                                objSearchResponse = getResponseDataFromQuery(arrIndex,
                                        new String[]{strType}, lstSourceField.toArray(new String[lstSourceField.size()]),
                                        lstFilters, bIsReversedFilter, intFromRow, intNumRow, lstFieldModel, new ArrayList<>(), lstSortingField);
                            }

                            lstSourceField.add("_id");

                            objQueryResult.setSearch_response(objSearchResponse);
                            objQueryResult.setTotal_fields(lstFieldModel.size() + 1);
                            objQueryResult.setNum_selected_fields(lstSourceField.size());
                            objQueryResult.setSelected_fields(lstSourceField);
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return objQueryResult;
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
                    objSearchSourceBuilder.query(objQueryBuilder);
                }
            }

            if (arrSource != null && arrSource.length > 0) {
                objSearchSourceBuilder.fetchSource(arrSource, null);
            }

            objRequestBuilder.setSource(objSearchSourceBuilder);
            objSearchResponse = objRequestBuilder.get();
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return objSearchResponse;
    }

    public List<SearchHit> getCustomQueryValue(List<String> lstIndex, String strType, QueryBuilder objCustomQueryBuilder, FieldSortBuilder objFieldSortBuilder, Integer intSize, Boolean bShouldRefresh) {
        List<SearchHit> lstHit = new ArrayList<>();

        try {
            if (objESClient != null && objCustomQueryBuilder != null) {
                List<String> lstExistedIndex = objESConnection.checkIndexArrayExisted(lstIndex);

                if (lstExistedIndex != null && lstExistedIndex.size() > 0) {
                    //Refresh index before export
                    if (bShouldRefresh) {
                        objESConnection.refreshIndexArray(lstExistedIndex);
                    }

                    String[] arrIndex = lstExistedIndex.toArray(new String[lstExistedIndex.size()]);

                    if (intSize == -1) {
                        List<String> lstScrollId = new ArrayList<>();
                        SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                        objSearchSourceBuilder.query(objCustomQueryBuilder);

                        SearchResponse objSearchResponse = objESClient.prepareSearch(arrIndex).setTypes(strType)
                                .setSource(objSearchSourceBuilder)
                                .addSort(objFieldSortBuilder)
                                .setScroll(new TimeValue(lScrollTTL))
                                .setSize(20000).get();

                        do {
                            if (objSearchResponse != null && objSearchResponse.getHits() != null
                                    && objSearchResponse.getHits().getTotalHits() > 0
                                    && objSearchResponse.getHits().getHits() != null
                                    && objSearchResponse.getHits().getHits().length > 0) {
                                List<SearchHit> lstCurHit = Arrays.asList(objSearchResponse.getHits().getHits());

                                lstHit.addAll(lstCurHit);

                                objSearchResponse = objESClient.prepareSearchScroll(objSearchResponse.getScrollId())
                                        .setScroll(new TimeValue(lScrollTTL)).get();

                                lstScrollId.add(objSearchResponse.getScrollId());
                            } else {
                                break;
                            }
                        } while (objSearchResponse.getHits() != null && objSearchResponse.getHits().getTotalHits() > 0
                                && objSearchResponse.getHits().getHits() != null
                                && objSearchResponse.getHits().getHits().length > 0);

                        objESConnection.deleteScrollId(lstScrollId);
                    } else if (intSize <= 1000000000) {
                        SearchSourceBuilder objSearchSourceBuilder = new SearchSourceBuilder();
                        objSearchSourceBuilder.query(objCustomQueryBuilder);

                        SearchResponse objSearchResponse = objESClient.prepareSearch(arrIndex).setTypes(strType)
                                .setSource(objSearchSourceBuilder)
                                .addSort(objFieldSortBuilder)
                                .setSize(intSize).get();

                        if (objSearchResponse != null && objSearchResponse.getHits() != null
                                && objSearchResponse.getHits().getTotalHits() > 0
                                && objSearchResponse.getHits().getHits() != null
                                && objSearchResponse.getHits().getHits().length > 0) {
                            List<SearchHit> lstCurHit = Arrays.asList(objSearchResponse.getHits().getHits());
                            lstHit.addAll(lstCurHit);
                        }
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.debug(ExceptionUtil.getStackTrace(objEx));
        }

        return lstHit;
    }
}
