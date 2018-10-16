package org.chronotics.pithos.ext.es.model;

import org.chronotics.pithos.ext.es.util.ESFilterOperationConstant;

import java.util.List;

public class ESPrepActionTypeModel {
    String id;
    String value;
    String category_id = ESFilterOperationConstant.PREP_OP_CATEGORY_CHANGE_TO;
    String category_name = ESFilterOperationConstant.PREP_OP_CATEGORY_NAME_CHANGE_TO;
    List<ESPrepActionModel> actions;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<ESPrepActionModel> getActions() {
        return actions;
    }

    public void setActions(List<ESPrepActionModel> actions) {
        this.actions = actions;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public String getCategory_name() {
        return category_name;
    }

    public void setCategory_name(String category_name) {
        this.category_name = category_name;
    }
}
