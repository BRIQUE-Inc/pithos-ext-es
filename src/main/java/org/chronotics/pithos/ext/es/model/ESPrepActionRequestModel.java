package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepActionRequestModel {
    Long action_idx;
    String action_type;
    String action_id;
    String new_field_name;
    List<String> data_values;
    List<ESPrepActionCustomValueRequestModel> user_values;

    public Long getAction_idx() {
        return action_idx;
    }

    public void setAction_idx(Long action_idx) {
        this.action_idx = action_idx;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public String getAction_id() {
        return action_id;
    }

    public void setAction_id(String action_id) {
        this.action_id = action_id;
    }

    public List<String> getData_values() {
        return data_values;
    }

    public void setData_values(List<String> data_values) {
        this.data_values = data_values;
    }

    public List<ESPrepActionCustomValueRequestModel> getUser_values() {
        return user_values;
    }

    public void setUser_values(List<ESPrepActionCustomValueRequestModel> user_values) {
        this.user_values = user_values;
    }

    public String getNew_field_name() {
        return new_field_name;
    }

    public void setNew_field_name(String new_field_name) {
        this.new_field_name = new_field_name;
    }
}
