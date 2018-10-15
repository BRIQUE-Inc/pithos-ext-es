package org.chronotics.pithos.ext.es.model;

public class ESFieldModel {
    String full_name;
    String type;
    Boolean fielddata;

    public String getFull_name() {
        return full_name;
    }
    public void setFull_name(String full_name) {
        this.full_name = full_name;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public Boolean getFielddata() {
        return fielddata;
    }
    public void setFielddata(Boolean fielddata) {
        this.fielddata = fielddata;
    }
}
