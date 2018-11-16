package org.chronotics.pithos.ext.es.model;

public class ESFileModel {
    String file_name;
    String file_path;
    Long file_size;
    Long processed_time;

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public Long getFile_size() {
        return file_size;
    }

    public void setFile_size(Long file_size) {
        this.file_size = file_size;
    }

    public Long getProcessed_time() {
        return processed_time;
    }

    public void setProcessed_time(Long processed_time) {
        this.processed_time = processed_time;
    }
}
