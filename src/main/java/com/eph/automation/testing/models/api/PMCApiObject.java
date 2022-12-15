package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * //updated by Nishant @ 04 Feb 2021, EPHD-2747
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PMCApiObject {

    private String code;
    public String getCode() {return code;}
    public void setCode(String code) {this.code = code;}

    private String name;
    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    private HashMap<String, Object> pmg;
    public HashMap<String, Object> getPmg() {return pmg;}
    public void setPmg(HashMap<String, Object> pmg) {this.pmg = pmg;}
}
