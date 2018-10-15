package org.chronotics.pithos.ext.es.model;

import java.util.ArrayList;
import java.util.List;

public class ESPrepActionModel {
    String action_id;
    String action_name;
    String data_type;
    Boolean is_show;
    List<String> predefined_values;
    ESFilterCustomModel custom_value;

    public String getAction_id() {
        return action_id;
    }

    public void setAction_id(String action_id) {
        this.action_id = action_id;
    }

    public String getAction_name() {
        return action_name;
    }

    public void setAction_name(String action_name) {
        this.action_name = action_name;
    }

    public String getData_type() {
        return data_type;
    }

    public void setData_type(String data_type) {
        this.data_type = data_type;
    }

    public Boolean getIs_show() {
        return is_show;
    }

    public void setIs_show(Boolean is_show) {
        this.is_show = is_show;
    }

    public List<String> getPredefined_values() {
        return predefined_values;
    }

    public void setPredefined_values(List<String> predefined_values) {
        this.predefined_values = predefined_values;
    }

    public ESFilterCustomModel getCustom_value() {
        return custom_value;
    }

    public void setCustom_value(ESFilterCustomModel custom_value) {
        this.custom_value = custom_value;
    }

    public ESPrepActionModel() {
    }

    public ESPrepActionModel(ESPrepActionModel objModel) {
        this.action_id = objModel.getAction_id();
        this.action_name = objModel.getAction_name();
        this.data_type = objModel.getData_type();
        this.is_show = objModel.getIs_show();

        if (objModel.getPredefined_values() != null) {
            this.predefined_values = new ArrayList<>(objModel.getPredefined_values());
        }

        if (objModel.getCustom_value() != null) {
            this.setCustom_value(objModel.getCustom_value());
        }
    }
}
