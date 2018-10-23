package org.chronotics.pithos.ext.es.model;

import java.util.ArrayList;
import java.util.List;

public class ESFilterAllRequestModel {
    Boolean is_reversed;
    List<String> selected_fields = new ArrayList<>();
    List<String> deleted_rows = new ArrayList<>();
    List<ESFilterRequestModel> filters = new ArrayList<>();

    public Boolean getIs_reversed() {
        return is_reversed;
    }

    public void setIs_reversed(Boolean is_reversed) {
        this.is_reversed = is_reversed;
    }

    public List<String> getSelected_fields() {
        return selected_fields;
    }

    public void setSelected_fields(List<String> selected_fields) {
        this.selected_fields = selected_fields;
    }

    public List<ESFilterRequestModel> getFilters() {
        return filters;
    }

    public void setFilters(List<ESFilterRequestModel> filters) {
        this.filters = filters;
    }

    public List<String> getDeleted_rows() {
        return deleted_rows;
    }

    public void setDeleted_rows(List<String> deleted_rows) {
        this.deleted_rows = deleted_rows;
    }
}
