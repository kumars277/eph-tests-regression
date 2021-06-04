package com.eph.automation.testing.models.api;

import java.util.HashMap;

//created by Nishant @ 19 May 2021, EPHD-3122
public class WorkAccessModels {

    private HashMap<String,String> accessModel;
    public HashMap<String, String> getAccessModel() {return accessModel;}
    public void setAccessModel(HashMap<String, String> accessModel) {this.accessModel = accessModel;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}
}
