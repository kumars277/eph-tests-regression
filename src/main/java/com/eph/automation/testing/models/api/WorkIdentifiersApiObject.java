package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 15 Apr 2020
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkIdentifiersApiObject {
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

    private String effectiveEndDate;
    public String getEffectiveEndDate() {return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    private void getWorkIdentifierByID(String workidentifierID){
        String sql = APIDataSQL.GET_GD_DATA_WORKIDENTIFIER_BY_IDENTIFIER.replace("PARAM1", workidentifierID);
        DBworkIdentifier = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        //updated by Nishant @ 18 May 2021, EPHD-3122
        Log.info("verifiying work identifiers... "+this.identifier);
        getWorkIdentifierByID(this.identifier);
        boolean identifierMatched = false;
        for(int i=0;i<this.DBworkIdentifier.size();i++)
        {
            if(this.identifierType.get("code").toString().equalsIgnoreCase(this.DBworkIdentifier.get(i).getF_TYPE()))
            {
                identifierMatched=true;

                printLog("work identifier code");

                Assert.assertEquals(getBreadcrumbMessage() +"-> "+ this.identifier+" - work identifier",this.identifierType.get("name"), getWorkIdentifierName(identifierType.get("code").toString()));
                printLog("work identifier name");

                Assert.assertEquals(getBreadcrumbMessage() +"-> "+ this.identifier+" - work identifier",effectiveStartDate, this.DBworkIdentifier.get(i).getIDENTIFIER_EFFECTIVE_START_DATE());
                printLog("work identifier effectiveStartDate");

                if(effectiveEndDate!=null|DBworkIdentifier.get(i).getIDENTIFIER_EFFECTIVE_END_DATE()!=null)
                {
                    Assert.assertEquals(getBreadcrumbMessage() +"-> "+ this.identifier+" - work identifier",effectiveEndDate, this.DBworkIdentifier.get(i).getIDENTIFIER_EFFECTIVE_END_DATE());
                    printLog("work identifier effectiveEndtDate");
                }

                break;
            }
        }
        Assert.assertTrue(getBreadcrumbMessage() +"-> "+ this.identifier+" - work identifier",identifierMatched);
    }

    private String getWorkIdentifierName(String code){//created by Nishant @ 18 May 2021, EPHD-3122
        String sql = String.format(APIDataSQL.SELECT_GD_LOV_DESCRIPTION_OF_IDENTIFIER,code);
        List<Map<String,Object>> workIdentifierName = DBManager.getDBResultMap(sql,Constants.EPH_URL);
        return (String) workIdentifierName.get(0).get("l_description");
    }


    private void printLog(String verified) {System.out.println("verified..." + verified);}
}