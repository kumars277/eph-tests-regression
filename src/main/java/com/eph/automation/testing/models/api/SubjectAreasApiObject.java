package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
class SubjectAreasApiObject {

    public SubjectAreasApiObject() {
    }
    public void compareWithDB(){

    }

    public SubjectAreaApiObject getSubjectArea() {
        return subjectArea;
    }

    public void setSubjectArea(SubjectAreaApiObject subjectArea) {
        this.subjectArea = subjectArea;
    }

    public Boolean getPrimary() {
        return primary;
    }

    public void setPrimary(Boolean primary) {
        this.primary = primary;
    }

    public String getEffectiveStartDate() {
        return effectiveStartDate;
    }

    public void setEffectiveStartDate(String effectiveStartDate) {
        this.effectiveStartDate = effectiveStartDate;
    }

    private SubjectAreaApiObject subjectArea;
    private Boolean primary;
    private String effectiveStartDate;

}