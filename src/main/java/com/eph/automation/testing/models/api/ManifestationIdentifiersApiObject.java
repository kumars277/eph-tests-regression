package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
class ManifestationIdentifiersApiObject {
    private List<ManifestationIdentifierObject> manifestationIDentifiersDO;

    public void compareWithDB(){
        if(this.identifier!=null) {
            getManifestationIdentifierByID(this.identifier);
            Assert.assertEquals(this.identifierType.get("code"), manifestationIDentifiersDO.get(0).getF_type());
        }
    }
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    private String identifier;
        private HashMap<String, Object> identifierType;

        public ManifestationIdentifiersApiObject() {
        }

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
}