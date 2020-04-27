package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 15 Apr 2020 to adjust data model changes for search API
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationIdentifiersApiObject {
    public ManifestationIdentifiersApiObject() {}

    private List<ManifestationIdentifierObject> manifestationIDentifiersDO;

    private String identifier;
    public String getIdentifier() {return identifier;}
    public void setIdentifier(String identifier) {this.identifier = identifier;}

    private HashMap<String, Object> identifierType;
    public HashMap<String, Object> getIdentifierType() {
            return identifierType;
        }
    public void setIdentifierType(HashMap<String, Object> identifierType) {
            this.identifierType = identifierType;
        }

    private void getManifestationIdentifierByID(String identifierID){
        String sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_ID, identifierID);
        manifestationIDentifiersDO = DBManager.getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        Log.info("comparing manifestation identifier... "+this.identifier);
        Log.info("-identifier type");
        if(this.identifier!=null) {
            getManifestationIdentifierByID(this.identifier);
            Assert.assertEquals(this.identifierType.get("code"), manifestationIDentifiersDO.get(0).getF_type());
        }
    }

}