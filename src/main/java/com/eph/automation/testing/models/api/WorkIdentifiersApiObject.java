package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 15 Apr 2020
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkIdentifiersApiObject {
    public WorkIdentifiersApiObject() {}

    private  List<WorkDataObject> DBworkIdentifier;

    private String identifier;
    public String getIdentifier() {return identifier;}
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    private HashMap<String, Object> identifierType;
    public HashMap<String, Object> getIdentifierType() {
        return identifierType;
    }
    public void setIdentifierType(HashMap<String, Object> identifierType) {
        this.identifierType = identifierType;
    }

    private String effectiveStartDate;
    public String getEffectiveStartDate(){return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate){this.effectiveStartDate=effectiveStartDate;}


    private void getWorkIdentifierByID(String workidentifierID){
        String sql = APIDataSQL.getWorkIdentifiersDataFromGDByIdentifier.replace("PARAM1", workidentifierID);
        DBworkIdentifier = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        Log.info("verifiying work identifiers... "+this.identifier);
        getWorkIdentifierByID(this.identifier);
        Assert.assertEquals(this.identifierType.get("code"), this.DBworkIdentifier.get(0).getF_TYPE());
        Log.info("verified...work identifier type");
    }
}