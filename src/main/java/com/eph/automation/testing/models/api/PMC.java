package com.eph.automation.testing.models.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PMC {
    private String pmcCode;
    private String pmcName;
    private HashMap<String, Object> pmg;

    public PMC() {
    }

    public String getPmcCode() {
        return pmcCode;
    }

    public void setPmcCode(String pmcCode) {
        this.pmcCode = pmcCode;
    }

    public String getPmcName() {
        return pmcName;
    }

    public void setPmcName(String pmcName) {
        this.pmcName = pmcName;
    }

    public HashMap<String, Object> getPmg() {
        return pmg;
    }

    public void setPmg(HashMap<String, Object> pmg) {
        this.pmg = pmg;
    }
}
