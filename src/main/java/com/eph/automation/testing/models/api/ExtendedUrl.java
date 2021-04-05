package com.eph.automation.testing.models.api;

import java.util.HashMap;

/**
 *created  by Nishant @ 09 Feb 2021, EPHD-2747
 */

public class ExtendedUrl {

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private String url;
    public String getUrl() {return url;}
    public void setUrl(String url) {this.url = url;}

    private String urlTitle;
    public String getUrlTitle() {return urlTitle;}
    public void setUrlTitle(String urlTitle) {this.urlTitle = urlTitle;}
}
