package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESFieldAggModel {
    String field;
    ESFieldStatModel field_stats;
    List<ESFieldPointModel> data_points;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public ESFieldStatModel getField_stats() {
        return field_stats;
    }

    public void setField_stats(ESFieldStatModel field_stats) {
        this.field_stats = field_stats;
    }

    public List<ESFieldPointModel> getData_points() {
        return data_points;
    }

    public void setData_points(List<ESFieldPointModel> data_points) {
        this.data_points = data_points;
    }
}
