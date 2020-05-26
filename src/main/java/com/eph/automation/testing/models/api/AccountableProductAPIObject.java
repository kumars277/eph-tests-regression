package com.eph.automation.testing.models.api;

import java.util.HashMap;

/*
created by Nishant @ 22 Apr 2020
covering journal info
 */
public class AccountableProductAPIObject {

    private String glProductSegmentCode;
    public String getGlProductSegmentCode() {return glProductSegmentCode;}
    public void setGlProductSegmentCode(String glProductSegmentCode) {this.glProductSegmentCode = glProductSegmentCode;}

    private String glProductSegmentName;
    public String getGlProductSegmentName() {return glProductSegmentName;}
    public void setGlProductSegmentName(String glProductSegmentName) {this.glProductSegmentName = glProductSegmentName;}

    private HashMap<String,Object> glProductParentValue;
    public HashMap<String, Object> getGlProductParentValue() {return glProductParentValue;}
    public void setGlProductParentValue(HashMap<String, Object> glProductParentValue) {this.glProductParentValue = glProductParentValue;}




}
