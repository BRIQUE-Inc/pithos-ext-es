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

    @Override
    public boolean equals(Object that) {
        if (that instanceof ESFieldModel) {
            ESFieldModel objThat = (ESFieldModel)that;

            if (objThat.getFull_name().equals(this.full_name)
                && objThat.getType().equals(this.type)
                && objThat.getFielddata().equals(this.fielddata)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
