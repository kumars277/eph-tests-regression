package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PMCApiObject {
    private String pmcCode;
    private String pmcName;
    private HashMap<String, Object> pmg;

    private String sql;
    private static List<String> ids;

    public PMCApiObject() {
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
