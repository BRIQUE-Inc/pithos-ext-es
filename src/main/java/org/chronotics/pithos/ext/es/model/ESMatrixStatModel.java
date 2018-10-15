package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESMatrixStatModel {
    List<String> fields;
    List<ESMatrixFieldStatModel> field_stats;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<ESMatrixFieldStatModel> getField_stats() {
        return field_stats;
    }

    public void setField_stats(List<ESMatrixFieldStatModel> field_stats) {
        this.field_stats = field_stats;
    }
}
