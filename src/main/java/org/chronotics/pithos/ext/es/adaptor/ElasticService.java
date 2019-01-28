package org.chronotics.pithos.ext.es.adaptor;

import org.chronotics.pithos.ext.es.model.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticService {
    Integer intNumBulkAction = 20000;

    ElasticConnection objESConnection;
    ElasticFilter objESFilter;
    ElasticAction objESAction;
    ElasticCluster objESCluster;

    public static ElasticService instance;

    public ElasticService(String strESClusterName, String strESCoorNodeIP, Integer intESCoorNodePort, String strTransportUsername, String strTransportPassword) {
        this.objESConnection = ElasticConnection.getInstance(strESClusterName, strESCoorNodeIP, intESCoorNodePort, strTransportUsername, strTransportPassword);
        this.objESFilter = new ElasticFilter(this.objESConnection);
        this.objESAction = new ElasticAction(this.objESConnection, this.objESFilter, intNumBulkAction);
        this.objESCluster = new ElasticCluster(this.objESConnection);
    }

    public ElasticService(String strESClusterName, String strListESCoorNodeConnectionString, String strTransportUsername, String strTransportPassword) {
        this.objESConnection = ElasticConnection.getInstance(strESClusterName, strListESCoorNodeConnectionString, strTransportUsername, strTransportPassword);
        this.objESFilter = new ElasticFilter(this.objESConnection);
        this.objESAction = new ElasticAction(this.objESConnection, this.objESFilter, intNumBulkAction);
        this.objESCluster = new ElasticCluster(this.objESConnection);
    }

    public static ElasticService getInstance(String strESClusterName, String strESCoorNodeIP,
                                                Integer intESCoorNodePort) {
        if (instance == null) {
            synchronized (ElasticService.class) {
                if (instance == null) {
                    instance = new ElasticService(strESClusterName, strESCoorNodeIP, intESCoorNodePort, "", "");
                }
            }
        }

        return instance;
    }

    public static ElasticService getInstance(String strESClusterName, String strESCoorNodeIP,
                                             Integer intESCoorNodePort, String strTransportUsername, String strTransportPassword) {
        if (instance == null) {
            synchronized (ElasticService.class) {
                if (instance == null) {
                    instance = new ElasticService(strESClusterName, strESCoorNodeIP, intESCoorNodePort, strTransportUsername, strTransportPassword);
                }
            }
        }

        return instance;
    }

    public static ElasticService getInstance(String strESClusterName, String strListESCoorNodeConnectionString,
                                                String strTransportUsername, String strTransportPassword) {
        if (instance == null) {
            synchronized (ElasticConnection.class) {
                if (instance == null) {
                    instance = new ElasticService(strESClusterName, strListESCoorNodeConnectionString, strTransportUsername, strTransportPassword);
                }
            }
        }

        return instance;
    }

    public void setNumReplica(Integer intNumReplica) {
        this.objESConnection.intNumReplica = intNumReplica;
    }

    public void setIsUseHotWarm(Boolean bIsUseHotWarm) {
        this.objESConnection.bIsUseHotWarm = bIsUseHotWarm;
    }

    public void setNumShards(Integer intNumShards) {
        this.objESConnection.intNumShards = intNumShards;
    }

    public void setCompressionLevel(String strCompressionLevel) {
        this.objESConnection.strCompressionLevel = strCompressionLevel;
    }

    public void closeInstance() {
        objESConnection.closeInstance();
    }

    /**
     * Statistic Matrix of Index
     * @param strIndex
     * @param strType
     * @param objFilterAllRequestModel
     * @return
     */
    public ESMatrixStatModel statsMatrix(String strIndex, String strType, ESFilterAllRequestModel objFilterAllRequestModel) {
        return objESConnection.statsMatrix(strIndex, strType, objFilterAllRequestModel);
    }

    /**
     * Check index and type is existed
     * @param strIndex
     * @param strType
     * @return
     */
    public List<Boolean> checkIndexExisted(String strIndex, String strType) {
        return objESConnection.checkIndexExisted(strIndex, strType);
    }

    /**
     * Update Settings of Index
     * @param strIndex
     * @param mapUpdateSetting
     * @return
     */
    public Boolean updateSettingsOfIndex(String strIndex, HashMap<String, Integer> mapUpdateSetting) {
        return objESConnection.updateSettingsOfIndex(strIndex, mapUpdateSetting);
    }

    /**
     * Delete index
     * @param strIndex
     * @return
     */
    public Boolean deleteIndex(String strIndex) {
        return objESConnection.deleteIndex(strIndex);
    }

    /**
     * Create Index
     * @param strIndex
     * @param strType
     * @param lstData
     * @param strDateField
     * @param mapMappingField
     * @param bDelIndexIfExisted
     * @return
     */
    public Boolean createIndex(String strIndex, String strType, List<?> lstData, String strDateField,
                               HashMap<String, ESMappingFieldModel> mapMappingField, Boolean bDelIndexIfExisted) {
        return objESConnection.createIndex(strIndex, strType, lstData, strDateField, mapMappingField, bDelIndexIfExisted);
    }

    /**
     * Get all indices
     * @return
     */
    public List<ESIndexModel> getAllIndices() {
        return objESConnection.getAllIndices();
    }

    /**
     * Get list of fields of 1 index and 1 type
     * @param strIndex
     * @param strType
     * @param lstField
     * @param bIsCheckNull
     * @return
     */
    public List<ESFieldModel> getFieldsMetaData(String strIndex, String strType, List<String> lstField, Boolean bIsCheckNull) {
        return objESConnection.getFieldsMetaData(strIndex, strType, lstField, bIsCheckNull);
    }

    /**
     * Merge date from multiple indices
     * @param objMergingRequest
     * @return
     */
    public Boolean mergeDataFromIndices(MergingDataRequestModel objMergingRequest) {
        return objESConnection.mergeDataFromIndices(objMergingRequest);
    }

    /**
     * Get list of pre-defined matrix filter operations
     * @return
     */
    public Map<String, List<ESFilterOperationModel>> getMatrixFilterOperation() {
        return objESFilter.getMatrixFilterOperation();
    }

    /**
     * Get list of pre-defined matrix filter operations
     * @return
     */
    public Map<String, List<ESFilterOperationModel>> getFilterOperation() {
        return objESFilter.getFilterOperation();
    }

    /**
     * Search data in elastic search without sorting
     * @param strIndex
     * @param strType
     * @param strQuery
     * @param lstSelectedField
     * @param intFromRow
     * @param intNumRow
     * @param intFromField
     * @param intNumField
     * @param intStatsType: 0 - No Statistic, 1 - Simple Statistic, 2 - Complex Statistic
     * @param objFilterAllRequest
     * @return
     */
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(String strIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField, Integer intFromRow, Integer intNumRow, Integer intFromField,
                                                                   Integer intNumField, Integer intStatsType, ESFilterAllRequestModel objFilterAllRequest) {
        return searchDataWithFieldIdxAndRowIdx(strIndex, strType, strQuery, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField, intStatsType, objFilterAllRequest, null, false);
    }

    /**
     * Search data in elastic search with sorting
     * @param strIndex
     * @param strType
     * @param strQuery
     * @param lstSelectedField
     * @param intFromRow
     * @param intNumRow
     * @param intFromField
     * @param intNumField
     * @param intStatsType: 0 - No Statistic, 1 - Simple Statistic, 2 - Complex Statistic
     * @param objFilterAllRequest
     * @return
     */
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(String strIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField, Integer intFromRow, Integer intNumRow, Integer intFromField,
                                                                   Integer intNumField, Integer intStatsType, ESFilterAllRequestModel objFilterAllRequest, List<ESSortingField> lstSortingField) {
        return objESFilter.searchDataWithFieldIdxAndRowIdx(strIndex, strType, strQuery, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField, intStatsType, objFilterAllRequest, lstSortingField);
    }

    /**
     * Search data in elastic search without sorting
     * @param strIndex
     * @param strType
     * @param strQuery
     * @param lstSelectedField
     * @param intFromRow
     * @param intNumRow
     * @param intFromField
     * @param intNumField
     * @param intStatsType: 0 - No Statistic, 1 - Simple Statistic, 2 - Complex Statistic
     * @param objFilterAllRequest
     * @return
     */
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(String strIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField, Integer intFromRow, Integer intNumRow, Integer intFromField,
                                                                   Integer intNumField, Integer intStatsType, ESFilterAllRequestModel objFilterAllRequest, Boolean bIsRefresh) {
        return searchDataWithFieldIdxAndRowIdx(strIndex, strType, strQuery, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField, intStatsType, objFilterAllRequest, null, bIsRefresh);
    }

    /**
     * Search data in elastic search with sorting
     * @param strIndex
     * @param strType
     * @param strQuery
     * @param lstSelectedField
     * @param intFromRow
     * @param intNumRow
     * @param intFromField
     * @param intNumField
     * @param intStatsType: 0 - No Statistic, 1 - Simple Statistic, 2 - Complex Statistic
     * @param objFilterAllRequest
     * @return
     */
    public HashMap<String, Object> searchDataWithFieldIdxAndRowIdx(String strIndex, String strType, String strQuery,
                                                                   List<String> lstSelectedField, Integer intFromRow, Integer intNumRow, Integer intFromField,
                                                                   Integer intNumField, Integer intStatsType, ESFilterAllRequestModel objFilterAllRequest, List<ESSortingField> lstSortingField, Boolean bIsRefresh) {
        return objESFilter.searchDataWithFieldIdxAndRowIdx(strIndex, strType, strQuery, lstSelectedField, intFromRow, intNumRow, intFromField, intNumField, intStatsType, objFilterAllRequest, lstSortingField, bIsRefresh);
    }

    /**
     * Get list of pre-defined actions
     * @return
     */
    public Map<String, List<ESPrepActionTypeModel>> getPrepActionTypes() {
        return objESAction.getPrepActionTypes();
    }

    /**
     * Create new index from old index
     * @param strIndex
     * @param strType
     * @param strFromIndex
     * @param strFromType
     * @param lstRemoveField
     * @param mapCopyField
     * @return
     */
    public Boolean createIndexFromOtherIndex(String strIndex, String strType, String strFromIndex, String strFromType,
                                             List<String> lstRemoveField, HashMap<String, String> mapCopyField) {
        return objESAction.createIndexFromOtherIndex(strIndex, strType, strFromIndex, strFromType, lstRemoveField, mapCopyField);
    }

    /**
     * Export data of index that has number of docs are under 10000
     * @param strIndex
     * @param strType
     * @param strFileName
     * @param intPageSize
     * @return
     */
    public String exportESDataToCSVUnder10000(String strIndex, String strType, String strFileName,
                                              Integer intPageSize) {
        return objESAction.exportESDataToCSVUnder10000(strIndex, strType, strFileName, intPageSize);
    }

    /**
     * Export data of index with some filter conditions
     * @param strIndex
     * @param strType
     * @param strFileName
     * @param intPageSize
     * @param objFilterAllRequest
     * @return
     */
    public String exportESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest) {
        return objESAction.exportESDataToCSV(strIndex, strType, strFileName, intPageSize, objFilterAllRequest);
    }

    /**
     * Export all data of index
     * @param strIndex
     * @param strType
     * @param strFileName
     * @param intPageSize
     * @return
     */
    public String exportESDataToCSV(String strIndex, String strType, String strFileName, Integer intPageSize) {
        return exportESDataToCSV(strIndex, strType, strFileName, intPageSize, null);
    }

    /**
     * Export all data of index pattern
     * @param strIndexPattern
     * @param strType
     * @param strFilePattern
     * @param intPageSize
     * @param objFilterAllRequest
     * @param bIsMultipleFile
     * @param inMaxFileLine
     * @return
     */
    public List<ESFileModel> exportESDataToCSV(String strIndexPattern, String strType, String strFilePattern, Integer intPageSize, ESFilterAllRequestModel objFilterAllRequest, Boolean bIsMultipleFile, Integer inMaxFileLine) {
        return objESAction.exportESDataToCSV(strIndexPattern, strType, strFilePattern, intPageSize, objFilterAllRequest, bIsMultipleFile, inMaxFileLine);
    }

    /**
     * Update index with some actions
     * @param lstPrepOp
     * @return
     */
    public Boolean prepESData(List<ESPrepAbstractModel> lstPrepOp) {
        return objESAction.prepESData(lstPrepOp);
    }

    /**
     * Delete field (physical) from index
     * @param strIndex
     * @param strType
     * @param strField
     * @return
     */
    public Boolean deleteField(String strIndex, String strType, String strField) {
        return objESAction.deleteField(strIndex, strType, strField);
    }

    /**
     * Insert data to ElasticSearch with Bulk Mode and pre-defined ID prefix (optinal)
     * @param strIndex
     * @param strType
     * @param lstData
     * @param strFieldDate
     * @param lstFieldModel
     * @param bIsUsedAutoID
     * @param strDocIdPrefix
     * @return
     */
    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel, Boolean bIsUsedAutoID, String strDocIdPrefix) {
        return objESAction.insertBulkData(strIndex, strType, lstData, strFieldDate, lstFieldModel, bIsUsedAutoID, strDocIdPrefix);
    }

    /**
     * Insert data to ElasticSearch with BulkMode and pre-defined IDs
     * @param strIndex
     * @param strType
     * @param lstData
     * @param strIDField
     * @param strFieldDate
     * @param lstFieldModel
     * @return
     */
    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strIDField, String strFieldDate, List<ESFieldModel> lstFieldModel) {
        return objESAction.insertBulkData(strIndex, strType, lstData, strIDField, strFieldDate, lstFieldModel);
    }

    /**
     * Insert data to ElasticSearch
     * @param strIndex
     * @param strType
     * @param lstData
     * @param strFieldDate
     * @param lstFieldModel
     * @return
     */
    public Boolean insertBulkData(String strIndex, String strType, List<?> lstData, String strFieldDate, List<ESFieldModel> lstFieldModel) {
        return insertBulkData(strIndex, strType, lstData, strFieldDate, lstFieldModel, false, "");
    }

    /**
     * Update data back to ElasticSearch
     * @param strIndex
     * @param strType
     * @param lstData
     * @param strIDField
     * @return
     */
    public Boolean updateBulkData(String strIndex, String strType, List<?> lstData, String strIDField) {
        return objESAction.updateBulkData(strIndex, strType, lstData, strIDField);
    }

    /**
     * Update data back to ElasticSearch with Custom Query
     * @param strIndex
     * @param strType
     * @param objFilterAllRequest
     * @param mapUpdateFieldValue
     * @param intPageSize
     * @return
     */
    public Boolean updateBulkData(String strIndex, String strType,
                                  ESFilterAllRequestModel objFilterAllRequest, HashMap<String, Object> mapUpdateFieldValue,
                                  Integer intPageSize) {
        return objESAction.updateBulkData(strIndex, strType, objFilterAllRequest, mapUpdateFieldValue, intPageSize);
    }

    /**
     * Delete data from ElasticSearch with custom query
     * @param strIndex
     * @param strType
     * @param objFilterAllRequest
     * @param intPageSize
     * @return
     */
    public Boolean deleteBulkData(String strIndex, String strType,
                                  ESFilterAllRequestModel objFilterAllRequest, Integer intPageSize) {
        return objESAction.deleteBulkData(strIndex, strType, objFilterAllRequest, intPageSize);
    }

    /**
     * Health Check Nodes' Statuses
     * @return
     */
    public HashMap<String, Object> healthCheckNodes() {
        return objESCluster.healthCheckNode();
    }

    /**
     * Get values from custom aggregation filter
     * @param strIndex
     * @param strType
     * @param objCustomAggregationBuilder
     * @return
     */
    public SearchResponse getCustomAggregationValue(String strIndex, String strType, QueryBuilder objCustomQueryBuilder, AggregationBuilder objCustomAggregationBuilder) {
        return objESFilter.getCustomAggregationValue(strIndex, strType, objCustomQueryBuilder, objCustomAggregationBuilder);
    }

    /**
     * Custom Aggs List
     * @param strIndex
     * @param strType
     * @param objCustomQueryBuilder
     * @param lstCustomAggregationBuilder
     * @return
     */
    public SearchResponse getCustomAggregationValue(String strIndex, String strType, QueryBuilder objCustomQueryBuilder, List<AggregationBuilder> lstCustomAggregationBuilder) {
        return objESFilter.getCustomAggregationValue(strIndex, strType, objCustomQueryBuilder, lstCustomAggregationBuilder);
    }

    /**
     * Get hits from custom query
     * @param strIndex
     * @param strType
     * @param objCustomQueryBuilder
     * @param objFieldSortBuilder
     * @return
     */
    public List<SearchHit> getCustomQueryValue(String strIndex, String strType, QueryBuilder objCustomQueryBuilder, FieldSortBuilder objFieldSortBuilder) {
        return objESFilter.getCustomQueryValue(strIndex, strType, objCustomQueryBuilder, objFieldSortBuilder, -1);
    }

    /**
     * Get hits from custom query
     * @param strIndex
     * @param strType
     * @param objCustomQueryBuilder
     * @param objFieldSortBuilder
     * @param intSize == -1: Get all data, < 1000000000: Get some data
     * @return
     */
    public List<SearchHit> getCustomQueryValue(String strIndex, String strType, QueryBuilder objCustomQueryBuilder, FieldSortBuilder objFieldSortBuilder, Integer intSize) {
        return objESFilter.getCustomQueryValue(strIndex, strType, objCustomQueryBuilder, objFieldSortBuilder, intSize);
    }
}
