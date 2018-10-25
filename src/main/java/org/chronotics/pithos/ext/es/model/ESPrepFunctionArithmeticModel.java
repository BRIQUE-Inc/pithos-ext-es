package org.chronotics.pithos.ext.es.model;

public class ESPrepFunctionArithmeticModel extends ESPrepAbstractModel {
    String field;
    String arithmetic_op;
    String arithmetic_param_1;
    String arithmetic_param_2;
    String new_field_name;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getArithmetic_op() {
        return arithmetic_op;
    }

    public void setArithmetic_op(String arithmetic_op) {
        this.arithmetic_op = arithmetic_op;
    }

    public String getArithmetic_param_1() {
        return arithmetic_param_1;
    }

    public void setArithmetic_param_1(String arithmetic_param_1) {
        this.arithmetic_param_1 = arithmetic_param_1;
    }

    public String getArithmetic_param_2() {
        return arithmetic_param_2;
    }

    public void setArithmetic_param_2(String arithmetic_param_2) {
        this.arithmetic_param_2 = arithmetic_param_2;
    }

    public String getNew_field_name() {
        return new_field_name;
    }

    public void setNew_field_name(String new_field_name) {
        this.new_field_name = new_field_name;
    }
}
