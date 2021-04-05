package com.eph.automation.testing.models.dao.StitchingExtended;
import java.util.HashMap;

public class WorkExtUrlJson {

    private ExtendedUrl extendedUrl;
    public ExtendedUrl getExtendedUrl() {return extendedUrl;}
    public void setExtendedUrl(ExtendedUrl extendedMetric) {this.extendedUrl = extendedUrl;}

    public static class ExtendedUrl{
        private HashMap<String ,Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}


        private String url;
        private String urlTitle;


        public String getUrlTitle() {
            return urlTitle;}
        public void setUrlTitle(String urlTitle) {this.urlTitle = urlTitle;}

        public String getUrl() {
            return url;}
        public void setUrl(String metricUrl) {this.url = url;}

    }
}




