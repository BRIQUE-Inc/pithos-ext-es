package org.chronotics.pithos.ext.es.util;

import org.chronotics.pithos.ext.es.log.Logger;
import org.chronotics.pithos.ext.es.log.LoggerFactory;
import org.chronotics.pithos.ext.es.model.ESFieldModel;
import org.chronotics.pithos.ext.es.model.ESFilterRequestModel;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ESFilterConverterUtil {
    private static Logger objLogger = LoggerFactory.getLogger(ESFilterConverterUtil.class);

    public static List<Object> createBooleanQueryBuilders(List<ESFilterRequestModel> lstESFilterRequest,
                                                          List<ESFieldModel> lstFields, List<String> lstDeletedRows) {
        List<Object> lstResult = new ArrayList<>();

        BoolQueryBuilder objBoolQueryBuilder = new BoolQueryBuilder();
        List<ESFilterRequestModel> lstNotAddedESFilterRequest = new ArrayList<>();

        try {
            for (int intCount = 0; intCount < lstESFilterRequest.size(); intCount++) {
                List<Object> lstReturn = createEachBooleanQueryBuilder(objBoolQueryBuilder, lstESFilterRequest.get(intCount), lstFields, lstDeletedRows);

                objBoolQueryBuilder = (BoolQueryBuilder)lstReturn.get(0);
                Boolean bIsAdded = (Boolean)lstReturn.get(1);

                if (!bIsAdded) {
                    lstNotAddedESFilterRequest.add(lstESFilterRequest.get(intCount));
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        lstResult.add(objBoolQueryBuilder);
        lstResult.add(lstNotAddedESFilterRequest);

        return lstResult;
    }

    private static Boolean checkSpecialCondition(ESFilterRequestModel objESFilterRequest) {
        Boolean bIsSpecialCondition = false;

        if (objESFilterRequest != null && objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
            String strCondition = objESFilterRequest.getFiltered_conditions().get(0);

            bIsSpecialCondition = strCondition.equals(ESFilterOperationConstant.FILTER_OUTLIER_MILD)
                                    || strCondition.equals(ESFilterOperationConstant.FILTER_OUTLIER_EXTREME)
                                    || strCondition.equals(ESFilterOperationConstant.FILTER_LCL)
                                    || strCondition.equals(ESFilterOperationConstant.FILTER_UCL)
                                    || strCondition.equals(ESFilterOperationConstant.FILTER_LCL_UCL);
        }

        return bIsSpecialCondition;
    }

    private static List<Object> createEachBooleanQueryBuilder(BoolQueryBuilder objQueryBuilder, ESFilterRequestModel objESFilterRequest,
                                                              List<ESFieldModel> lstFields, List<String> lstDeletedRows) {
        List<Object> lstReturn = new ArrayList<>();
        Boolean bIsAdded = true;

        if (objESFilterRequest != null) {
            String strFieldOfIndex = checkFieldName(objESFilterRequest, lstFields);

            Boolean bIsSpecialCondition = checkSpecialCondition(objESFilterRequest);

            if (strFieldOfIndex != null && !strFieldOfIndex.isEmpty()) {
                switch (objESFilterRequest.getFiltered_operation()) {
                    case ESFilterOperationConstant.IS:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            if (!bIsSpecialCondition) {
                                objQueryBuilder.must(QueryBuilders.matchQuery(strFieldOfIndex, objESFilterRequest.getFiltered_conditions().get(0)));
                            } else {
                                bIsAdded = false;
                            }
                        }

                        break;
                    case ESFilterOperationConstant.IS_NOT:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            if (!bIsSpecialCondition) {
                                objQueryBuilder.mustNot(QueryBuilders.matchQuery(strFieldOfIndex, objESFilterRequest.getFiltered_conditions().get(0)));
                            } else {
                                bIsAdded = false;
                            }
                        }
                        break;
                    case ESFilterOperationConstant.IS_ONE_OF:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            StringBuilder objORCondition = new StringBuilder();

                            for (int intCount = 0; intCount < objESFilterRequest.getFiltered_conditions().size(); intCount++) {
                                if (intCount > 0) {
                                    objORCondition.append(" OR ");
                                }

                                objORCondition.append("(").append(objESFilterRequest.getFiltered_conditions().get(intCount)).append(")");
                            }

                            objQueryBuilder.must(QueryBuilders.queryStringQuery(objORCondition.toString()).field(strFieldOfIndex));
                        }
                        break;
                    case ESFilterOperationConstant.IS_NOT_ONE_OF:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            StringBuilder objORCondition = new StringBuilder();

                            for (int intCount = 0; intCount < objESFilterRequest.getFiltered_conditions().size(); intCount++) {
                                if (intCount > 0) {
                                    objORCondition.append(" AND ");
                                }

                                objORCondition.append("(").append(objESFilterRequest.getFiltered_conditions().get(intCount)).append(")");
                            }

                            objQueryBuilder.mustNot(QueryBuilders.queryStringQuery(objORCondition.toString()).field(strFieldOfIndex));
                        }
                        break;
                    case ESFilterOperationConstant.IS_BETWEEN:
                        if (bIsSpecialCondition) {
                            bIsAdded = false;
                        } else if (objESFilterRequest.getFrom_range_condition() != null || objESFilterRequest.getTo_range_condition() != null) {
                            RangeQueryBuilder objRangeQueryBuilder = QueryBuilders.rangeQuery(strFieldOfIndex );

                            if (objESFilterRequest.getFrom_range_condition() != null) {
                                objRangeQueryBuilder = objRangeQueryBuilder.from(objESFilterRequest.getFrom_range_condition(), true);
                            }

                            if (objESFilterRequest.getTo_range_condition() != null) {
                                objRangeQueryBuilder = objRangeQueryBuilder.to(objESFilterRequest.getTo_range_condition(), true);
                            }

                            objQueryBuilder.must(objRangeQueryBuilder);
                        }
                        break;
                    case ESFilterOperationConstant.IS_NOT_BETWEEN:
                        if (bIsSpecialCondition) {
                            bIsAdded = false;
                        } else if (objESFilterRequest.getFrom_range_condition() != null || objESFilterRequest.getTo_range_condition() != null) {
                            RangeQueryBuilder objRangeQueryBuilder = QueryBuilders.rangeQuery(strFieldOfIndex);

                            if (objESFilterRequest.getFrom_range_condition() != null) {
                                objRangeQueryBuilder = objRangeQueryBuilder.from(objESFilterRequest.getFrom_range_condition(), true);
                            }

                            if (objESFilterRequest.getTo_range_condition() != null) {
                                objRangeQueryBuilder = objRangeQueryBuilder.to(objESFilterRequest.getTo_range_condition(), true);
                            }

                            objQueryBuilder.mustNot(objRangeQueryBuilder);
                        }
                        break;
                    case ESFilterOperationConstant.EXISTS:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            objQueryBuilder.must(QueryBuilders.wildcardQuery(strFieldOfIndex, new StringBuilder().append("*").append(objESFilterRequest.getFiltered_conditions().get(0)).append("*").toString()));
                        }
                        break;
                    case ESFilterOperationConstant.DOES_NOT_EXIST:
                        if (objESFilterRequest.getFiltered_conditions() != null && objESFilterRequest.getFiltered_conditions().size() > 0) {
                            objQueryBuilder.mustNot(QueryBuilders.wildcardQuery(strFieldOfIndex, new StringBuilder().append("*").append(objESFilterRequest.getFiltered_conditions().get(0)).append("*").toString()));
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        // TODO the row _id must not in the array of deleted rows
        if(lstDeletedRows != null && lstDeletedRows.size() > 0) {
            objQueryBuilder.mustNot(QueryBuilders.termsQuery("_id", lstDeletedRows));
        }

        lstReturn.add(objQueryBuilder);
        lstReturn.add(bIsAdded);

        return lstReturn;
    }

    public static String checkFieldName(ESFilterRequestModel objESFilterRequest, List<ESFieldModel> lstFields) {
        String strFieldName = "";

        if (objESFilterRequest != null) {
            List<String> lstCheckField = lstFields.stream().filter(objField -> objField.getFull_name().trim().toLowerCase().equals(objESFilterRequest.getFiltered_on_field().trim().toLowerCase()))
                    .map(objFiltered -> objFiltered.getFull_name()).collect(Collectors.toList());

            if (lstCheckField != null && lstCheckField.size() > 0) {
                strFieldName = lstCheckField.get(0);
            }
        }

        return strFieldName;
    }
}
