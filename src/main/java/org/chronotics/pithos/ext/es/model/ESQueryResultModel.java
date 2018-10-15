package org.chronotics.pithos.ext.es.model;

import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

public class ESQueryResultModel {
    SearchResponse search_response;
    Integer total_fields;
    Integer num_selected_fields;
    List<String> selected_fields;
    List<ESFieldAggModel> agg_fields;

    public SearchResponse getSearch_response() {
        return search_response;
    }
    public void setSearch_response(SearchResponse search_response) {
        this.search_response = search_response;
    }
    public Integer getTotal_fields() {
        return total_fields;
    }
    public void setTotal_fields(Integer total_fields) {
        this.total_fields = total_fields;
    }
    public Integer getNum_selected_fields() {
        return num_selected_fields;
    }
    public void setNum_selected_fields(Integer num_selected_fields) {
        this.num_selected_fields = num_selected_fields;
    }
    public List<String> getSelected_fields() {
        return selected_fields;
    }
    public void setSelected_fields(List<String> selected_fields) {
        this.selected_fields = selected_fields;
    }

    public List<ESFieldAggModel> getAgg_fields() {
        return agg_fields;
    }

    public void setAgg_fields(List<ESFieldAggModel> agg_fields) {
        this.agg_fields = agg_fields;
    }
}
