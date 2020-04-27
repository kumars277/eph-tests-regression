package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 30 Mar 2020 as per latest API changes
 */
import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.FinancialAttribsContext;
import com.eph.automation.testing.models.dao.FinancialAttribsDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.services.db.sql.FinAttrSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.HashMap;


@JsonIgnoreProperties(ignoreUnknown = true)
class FinancialAttributesApiObject {
    @StaticInjection
    private FinancialAttribsContext financialAttribs;
    public FinancialAttributesApiObject() {}

    private void getFinancialData(String workid){
        String sql = String.format(APIDataSQL.GET_GD_FinnAttr_DATA, workid);
        financialAttribs.financialDataFromGD = DBManager.getDBResultAsBeanList(sql, FinancialAttribsDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(String workID){
        Log.info("comparing below financial attributes for work... "+workID);
        Log.info("\n-company code\n"+
                "-costResponsibilityCentre code\n"+
                "-revenueResponsibilityCentre code"
        );
        getFinancialData(workID);
        Assert.assertEquals(financialAttribs.financialDataFromGD.get(0).getGl_company(), this.glCompany.get("code"));
        Assert.assertEquals(financialAttribs.financialDataFromGD.get(0).getCost_resp_centre(), this.costResponsibilityCentre.get("code"));
        Assert.assertEquals(financialAttribs.financialDataFromGD.get(0).getRevenue_resp_centre(), this.revenueResponsibilityCentre.get("code"));
    }


    private HashMap<String, Object> glCompany;
    public HashMap<String, Object> getGlCompany() {
        return glCompany;
    }
    public void setGlCompany(HashMap<String, Object> glCompany) {
        this.glCompany = glCompany;
    }

    private HashMap<String, Object> costResponsibilityCentre;
    public HashMap<String, Object> getCostResponsibilityCentre() {
        return costResponsibilityCentre;
    }
    public void setCostResponsibilityCentre(HashMap<String, Object> costResponsibilityCentre) {this.costResponsibilityCentre = costResponsibilityCentre;}

    private HashMap<String, Object> revenueResponsibilityCentre;
    public HashMap<String, Object> getRevenueResponsibilityCentre() {
        return revenueResponsibilityCentre;
    }
    public void setRevenueResponsibilityCentre(HashMap<String, Object> revenueResponsibilityCentre) {this.revenueResponsibilityCentre = revenueResponsibilityCentre;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {
        return effectiveStartDate;
    }
    public void setEffectiveStartDate(String effectiveStartDate) {
        this.effectiveStartDate = effectiveStartDate;
    }

}