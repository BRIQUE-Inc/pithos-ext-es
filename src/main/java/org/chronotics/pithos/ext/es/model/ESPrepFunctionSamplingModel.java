package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepFunctionSamplingModel extends ESPrepAbstractModel {
    String sampling_op;
    List<String> new_fields;
    List<String> selected_fields;
    Long num_of_rows;
    List<Double> defined_values; //replace | min,max,replace | step | mean,sd | min,max

    public String getSampling_op() {
        return sampling_op;
    }

    public void setSampling_op(String sampling_op) {
        this.sampling_op = sampling_op;
    }

    public List<String> getNew_fields() {
        return new_fields;
    }

    public void setNew_fields(List<String> new_fields) {
        this.new_fields = new_fields;
    }

    public List<String> getSelected_fields() {
        return selected_fields;
    }

    public void setSelected_fields(List<String> selected_fields) {
        this.selected_fields = selected_fields;
    }

    public Long getNum_of_rows() {
        return num_of_rows;
    }

    public void setNum_of_rows(Long num_of_rows) {
        this.num_of_rows = num_of_rows;
    }

    public List<Double> getDefined_values() {
        return defined_values;
    }

    public void setDefined_values(List<Double> defined_values) {
        this.defined_values = defined_values;
    }
}
