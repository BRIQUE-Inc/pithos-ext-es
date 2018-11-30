package org.chronotics.pithos.ext.es.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ESMappingFieldModel {
    @JsonProperty("type")
    String type;

    @JsonProperty("fielddata")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Boolean fielddata;

    @JsonProperty("copy_to")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String copy_to;

    @JsonProperty("index")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Boolean index;

    @JsonProperty("norms")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Boolean norms = false;

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

    public String getCopy_to() {
        return copy_to;
    }

    public void setCopy_to(String copy_to) {
        this.copy_to = copy_to;
    }

    public Boolean getIndex() {
        return index;
    }

    public void setIndex(Boolean index) {
        this.index = index;
    }

    public Boolean getNorms() {
        return norms;
    }

    public void setNorms(Boolean norms) {
        this.norms = norms;
    }
}
