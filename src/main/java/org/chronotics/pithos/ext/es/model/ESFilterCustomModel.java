package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESFilterCustomModel {
    Integer num_of_input;
    Boolean is_single_value;
    List<ESFilterTextDefaultModel> ui;

    public Integer getNum_of_input() {
        return num_of_input;
    }

    public void setNum_of_input(Integer num_of_input) {
        this.num_of_input = num_of_input;
    }

    public Boolean getIs_single_value() {
        return is_single_value;
    }

    public void setIs_single_value(Boolean is_single_value) {
        this.is_single_value = is_single_value;
    }

    public List<ESFilterTextDefaultModel> getUi() {
        return ui;
    }

    public void setUi(List<ESFilterTextDefaultModel> ui) {
        this.ui = ui;
    }
}
