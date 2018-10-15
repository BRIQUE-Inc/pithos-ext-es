package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepListActionRequestModel {
    String index;
    String type;
    List<ESPrepActionRequestModel> actions;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<ESPrepActionRequestModel> getActions() {
        return actions;
    }

    public void setActions(List<ESPrepActionRequestModel> actions) {
        this.actions = actions;
    }
}
