package org.chronotics.pithos.ext.es.model;

public class ESSortingField {
    String field_name;

    //1 - ASC, 2 - DESC
    Integer order_by;

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    public Integer getOrder_by() {
        return order_by;
    }

    public void setOrder_by(Integer order_by) {
        this.order_by = order_by;
    }
}
