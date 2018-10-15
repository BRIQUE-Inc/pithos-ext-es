package org.chronotics.pithos.ext.es.model;

import java.util.List;

public class ESPrepDocModel extends ESPrepAbstractModel {
    List<String> remove_doc_ids;
    List<String> copy_doc_ids;
    List<Integer> copy_doc_nums;

    public List<String> getRemove_doc_ids() {
        return remove_doc_ids;
    }

    public void setRemove_doc_ids(List<String> remove_doc_ids) {
        this.remove_doc_ids = remove_doc_ids;
    }

    public List<String> getCopy_doc_ids() {
        return copy_doc_ids;
    }

    public void setCopy_doc_ids(List<String> copy_doc_ids) {
        this.copy_doc_ids = copy_doc_ids;
    }

    public List<Integer> getCopy_doc_nums() {
        return copy_doc_nums;
    }

    public void setCopy_doc_nums(List<Integer> copy_doc_nums) {
        this.copy_doc_nums = copy_doc_nums;
    }
}
