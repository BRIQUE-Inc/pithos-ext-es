package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESFilterRequestModel {
    String filtered_on_field;
    Integer filtered_operation;
    List<Object> filtered_conditions;
    Double from_range_condition;
    Double to_range_condition;

    public String getFiltered_on_field() {
        return filtered_on_field;
    }

    public void setFiltered_on_field(String filtered_on_field) {
        this.filtered_on_field = filtered_on_field;
    }

    public Integer getFiltered_operation() {
        return filtered_operation;
    }

    public void setFiltered_operation(Integer filtered_operation) {
        this.filtered_operation = filtered_operation;
    }

    public List<Object> getFiltered_conditions() {
        return filtered_conditions;
    }

    public void setFiltered_conditions(List<Object> filtered_conditions) {
        this.filtered_conditions = filtered_conditions;
    }

    public Double getFrom_range_condition() {
        return from_range_condition;
    }

    public void setFrom_range_condition(Double from_range_condition) {
        this.from_range_condition = from_range_condition;
    }

    public Double getTo_range_condition() {
        return to_range_condition;
    }

    public void setTo_range_condition(Double to_range_condition) {
        this.to_range_condition = to_range_condition;
    }
}
