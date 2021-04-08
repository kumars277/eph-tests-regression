package com.eph.automation.testing.models.api;

import java.util.HashMap;

/**
 *created  by Nishant @ 09 Feb 2021, EPHD-2747
 */
public class ExtendedMetric {

    private HashMap<String,Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private String metric;
    public String getMetric() {return metric;}
    public void setMetric(String metric) {this.metric = metric;}

    private String metricUrl;
    public String getMetricUrl() {return metricUrl;}
    public void setMetricUrl(String metricUrl) {this.metricUrl = metricUrl;}

    private String metricYear;
    public String getMetricYear() {return metricYear;}
    public void setMetricYear(String metricYear) {this.metricYear = metricYear;}
}
