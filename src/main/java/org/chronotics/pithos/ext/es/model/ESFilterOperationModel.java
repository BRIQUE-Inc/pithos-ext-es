package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESFilterOperationModel {
    Integer op_id;
    String op_name;
    List<String> predefined_ops;
    ESFilterCustomModel custom_value;

    public Integer getOp_id() {
        return op_id;
    }

    public void setOp_id(Integer op_id) {
        this.op_id = op_id;
    }

    public String getOp_name() {
        return op_name;
    }

    public void setOp_name(String op_name) {
        this.op_name = op_name;
    }

    public List<String> getPredefined_ops() {
        return predefined_ops;
    }

    public void setPredefined_ops(List<String> predefined_ops) {
        this.predefined_ops = predefined_ops;
    }

    public ESFilterCustomModel getCustom_value() {
        return custom_value;
    }

    public void setCustom_value(ESFilterCustomModel custom_value) {
        this.custom_value = custom_value;
    }

    public ESFilterOperationModel(Integer op_id, String op_name, List<String> predefined_ops, ESFilterCustomModel custom_value) {
        this.op_id = op_id;
        this.op_name = op_name;
        this.predefined_ops = predefined_ops;
        this.custom_value = custom_value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ESFilterOperationModel) {
            ESFilterOperationModel objThat = (ESFilterOperationModel)obj;

            if (objThat.getOp_id() != null && objThat.getOp_id().equals(this.op_id)) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return op_id.hashCode();
    }
}
