package org.chronotics.pithos.ext.es.model;

public class ESPrepDataTypeChangeModel extends ESPrepAbstractModel {
    String field;
    String converted_data_type;
    Boolean is_forced;
    String failed_default_value;
    String date_format;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getConverted_data_type() {
        return converted_data_type;
    }

    public void setConverted_data_type(String converted_data_type) {
        this.converted_data_type = converted_data_type;
    }

    public Boolean getIs_forced() {
        return is_forced;
    }

    public void setIs_forced(Boolean is_forced) {
        this.is_forced = is_forced;
    }

    public String getFailed_default_value() {
        return failed_default_value;
    }

    public void setFailed_default_value(String failed_default_value) {
        this.failed_default_value = failed_default_value;
    }

    public String getDate_format() {
        return date_format;
    }

    public void setDate_format(String date_format) {
        this.date_format = date_format;
    }
}
