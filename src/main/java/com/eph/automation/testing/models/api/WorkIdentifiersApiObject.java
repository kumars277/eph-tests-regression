package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class WorkIdentifiersApiObject {
    private  List<WorkDataObject> DBworkIdentifier;

    public void compareWithDB(){
        getWorkIdentifierByID(this.identifier);
        Assert.assertEquals(this.identifierType.get("code"), this.DBworkIdentifier.get(0).getF_TYPE());
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    private String identifier;
        private HashMap<String, Object> identifierType;

        public WorkIdentifiersApiObject() {
        }


        public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }

        public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }

    private void getWorkIdentifierByID(String workidentifierID){
        String sql = APIDataSQL.getWorkIdentifiersDataFromGDByID
                .replace("PARAM1", workidentifierID);
        DBworkIdentifier = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }
}