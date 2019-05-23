package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class SubjectAreaApiObject {

    public SubjectAreaApiObject() {
    }
    public void compareWithDB(){

    }

    public HashMap<String, Object> getType() {
        return type;
    }

    public void setType(HashMap<String, Object> type) {
        this.type = type;
    }

    public HashMap<String, Object> getParentSubjectArea() {
        return parentSubjectArea;
    }

    public void setParentSubjectArea(HashMap<String, Object> parentSubjectArea) {
        this.parentSubjectArea = parentSubjectArea;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private HashMap<String, Object> type;
    private HashMap<String, Object> parentSubjectArea;
    private String code;
    private String name;

}