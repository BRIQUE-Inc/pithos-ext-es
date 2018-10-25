package org.chronotics.pithos.ext.es.model;

public class ESPrepFormatModel extends ESPrepAbstractModel {
    String field;
    String format_op;
    String format_param_1;
    String format_param_2;
    String new_field_name;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getFormat_op() {
        return format_op;
    }

    public void setFormat_op(String format_op) {
        this.format_op = format_op;
    }

    public String getFormat_param_1() {
        return format_param_1;
    }

    public void setFormat_param_1(String format_param_1) {
        this.format_param_1 = format_param_1;
    }

    public String getFormat_param_2() {
        return format_param_2;
    }

    public void setFormat_param_2(String format_param_2) {
        this.format_param_2 = format_param_2;
    }

    public String getNew_field_name() {
        return new_field_name;
    }

    public void setNew_field_name(String new_field_name) {
        this.new_field_name = new_field_name;
    }
}
