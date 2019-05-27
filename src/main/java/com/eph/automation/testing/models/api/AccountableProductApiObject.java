package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class AccountableProductApiObject {

    public AccountableProductApiObject() {
    }

    public void compareWithDB(){

    }

    private String glProductSegmentCode;

    public String getGlProductSegmentCode() {
        return glProductSegmentCode;
    }

    public void setGlProductSegmentCode(String glProductSegmentCode) {
        this.glProductSegmentCode = glProductSegmentCode;
    }

    public String getGlProductSegmentName() {
        return glProductSegmentName;
    }

    public void setGlProductSegmentName(String glProductSegmentName) {
        this.glProductSegmentName = glProductSegmentName;
    }

    public HashMap<String, Object> getGlProductParentValue() {
        return glProductParentValue;
    }

    public void setGlProductParentValue(HashMap<String, Object> glProductParentValue) {
        this.glProductParentValue = glProductParentValue;
    }

    private String glProductSegmentName;
    private HashMap<String, Object> glProductParentValue;

}