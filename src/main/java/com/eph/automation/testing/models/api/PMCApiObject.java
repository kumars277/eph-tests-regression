package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PMCApiObject {
    private String code;
    private String name;
    private HashMap<String, Object> pmg;

    private String sql;
    private static List<String> ids;

    public PMCApiObject() {    }

    public String getCode() {return code;}
    public void setCode(String code) {this.code = code;}

    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    public HashMap<String, Object> getPmg() {return pmg;}
    public void setPmg(HashMap<String, Object> pmg) {this.pmg = pmg;}
}
