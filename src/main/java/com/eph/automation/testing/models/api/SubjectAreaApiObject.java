package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 16 Feb 2021, EPHD-2747
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubjectAreaApiObject {

    public SubjectAreaApiObject() {}

    private String code;
    public String getCode() {return code;}
    public void setCode(String code) {this.code = code;}

    private String name;
    public String getName() {return name;}
    public void setName(String name) {this.name = name;}

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private HashMap<String, Object> parentSubjectArea;
    public HashMap<String, Object> getParentSubjectArea() {return parentSubjectArea;}
    public void setParentSubjectArea(HashMap<String, Object> parentSubjectArea) {this.parentSubjectArea = parentSubjectArea;}


}