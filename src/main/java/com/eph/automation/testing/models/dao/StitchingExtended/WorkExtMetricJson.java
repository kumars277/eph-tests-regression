package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class WorkExtMetricJson {

    private ExtendedMetric extendedMetric;
    public ExtendedMetric getExtendedMetric() {return extendedMetric;}
    public void setExtendedMetric(ExtendedMetric extendedMetric) {this.extendedMetric = extendedMetric;}

    public static class ExtendedMetric{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String metric;
        private String metricUrl;
        private String metricYear;

        public String getMetric() {
            return metric;}
        public void setMetric(String metric) {this.metric = metric;}

        public String getMetricURL() {
            return metricUrl;}
        public void setMetricURL(String metricUrl) {this.metricUrl = metricUrl;}

        public String getMetricYear() {
            return metricYear;}
        public void setMetricYear(String metricYear) {this.metricYear = metricYear;}


    }
}




