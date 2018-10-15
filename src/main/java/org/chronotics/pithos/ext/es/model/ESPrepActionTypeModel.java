package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepActionTypeModel {
    String id;
    String value;
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
}
