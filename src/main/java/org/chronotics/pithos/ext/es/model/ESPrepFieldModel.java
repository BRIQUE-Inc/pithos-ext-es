package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepFieldModel extends ESPrepAbstractModel {
    List<String> remove_fields;
    List<String> copy_from_fields;
    List<String> copy_to_fields;

    public List<String> getRemove_fields() {
        return remove_fields;
    }

    public void setRemove_fields(List<String> remove_fields) {
        this.remove_fields = remove_fields;
    }

    public List<String> getCopy_from_fields() {
        return copy_from_fields;
    }

    public void setCopy_from_fields(List<String> copy_from_fields) {
        this.copy_from_fields = copy_from_fields;
    }

    public List<String> getCopy_to_fields() {
        return copy_to_fields;
    }

    public void setCopy_to_fields(List<String> copy_to_fields) {
        this.copy_to_fields = copy_to_fields;
    }
}
