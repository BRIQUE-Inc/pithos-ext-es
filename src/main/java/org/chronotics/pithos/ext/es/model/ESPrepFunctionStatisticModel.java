package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepFunctionStatisticModel extends ESPrepAbstractModel {
    String statistic_op;
    List<String> selected_field;
    String new_field_name;

    public String getStatistic_op() {
        return statistic_op;
    }

    public void setStatistic_op(String statistic_op) {
        this.statistic_op = statistic_op;
    }

    public List<String> getSelected_field() {
        return selected_field;
    }

    public void setSelected_field(List<String> selected_field) {
        this.selected_field = selected_field;
    }

    public String getNew_field_name() {
        return new_field_name;
    }

    public void setNew_field_name(String new_field_name) {
        this.new_field_name = new_field_name;
    }
}
