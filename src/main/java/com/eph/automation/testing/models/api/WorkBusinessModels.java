package com.eph.automation.testing.models.api;

import java.util.HashMap;

//created by Nishant @ 19 May 2021, EPHD-3122
public class WorkBusinessModels {

    private HashMap<String,String> businessModel;
    public HashMap<String, String> getBusinessModel() {return businessModel;}
    public void setBusinessModel(HashMap<String, String> businessModel) {this.businessModel = businessModel;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}
}
