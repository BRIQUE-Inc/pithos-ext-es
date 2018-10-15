package org.chronotics.pithos.ext.es.model;

public class ESFieldPointModel {
    String point_key;
    Double point_value;
    Long point_num;
    Double point_percent;

    public String getPoint_key() {
        return point_key;
    }

    public void setPoint_key(String point_key) {
        this.point_key = point_key;
    }

    public Double getPoint_value() {
        return point_value;
    }

    public void setPoint_value(Double point_value) {
        this.point_value = point_value;
    }

    public Long getPoint_num() {
        return point_num;
    }

    public void setPoint_num(Long point_num) {
        this.point_num = point_num;
    }

    public Double getPoint_percent() {
        return point_percent;
    }

    public void setPoint_percent(Double point_percent) {
        this.point_percent = point_percent;
    }
}
