package org.chronotics.pithos.ext.es.model;


import java.util.List;

public class ESMatrixFieldStatModel {
    String field;
    Long count;
    Double mean;
    Double variance;
    Double skewness;
    Double kurtosis;
    List<Double> covariances;
    List<Double> correlations;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public Double getVariance() {
        return variance;
    }

    public void setVariance(Double variance) {
        this.variance = variance;
    }

    public Double getSkewness() {
        return skewness;
    }

    public void setSkewness(Double skewness) {
        this.skewness = skewness;
    }

    public Double getKurtosis() {
        return kurtosis;
    }

    public void setKurtosis(Double kurtosis) {
        this.kurtosis = kurtosis;
    }

    public List<Double> getCovariances() {
        return covariances;
    }

    public void setCovariances(List<Double> covariances) {
        this.covariances = covariances;
    }

    public List<Double> getCorrelations() {
        return correlations;
    }

    public void setCorrelations(List<Double> correlations) {
        this.correlations = correlations;
    }
}
