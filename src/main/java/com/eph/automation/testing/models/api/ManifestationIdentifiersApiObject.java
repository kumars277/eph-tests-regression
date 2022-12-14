package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 15 Apr 2020 to adjust data model changes for search API
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ManifestationIdentifierObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;

import java.util.HashMap;
import java.util.List;

import static com.eph.automation.testing.models.contexts.DataQualityContext.getBreadcrumbMessage;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestationIdentifiersApiObject {
    public ManifestationIdentifiersApiObject() {}

    private List<ManifestationIdentifierObject> manifestationIDentifiersDO;

    private String identifier;
    public String getIdentifier() {return identifier;}
    public void setIdentifier(String identifier) {this.identifier = identifier;}

    private HashMap<String, Object> identifierType;
    public HashMap<String, Object> getIdentifierType() {return identifierType;}
    public void setIdentifierType(HashMap<String, Object> identifierType) {this.identifierType = identifierType;}

    private String effectiveStartDate;
    public String getEffectiveStartDate() {return effectiveStartDate;}
    public void setEffectiveStartDate(String effectiveStartDate) {this.effectiveStartDate = effectiveStartDate;}

    private String effectiveEndDate;
    public String getEffectiveEndDate() { return effectiveEndDate;}
    public void setEffectiveEndDate(String effectiveEndDate) {this.effectiveEndDate = effectiveEndDate;}

    private void getManifestationIdentifierByID(String identifierID, String manifestionId) {
        String sql = String.format(APIDataSQL.SELECT_GD_MANIFESTATION_IDENTIFIER_BY_ID, identifierID,manifestionId);
        manifestationIDentifiersDO = DBManager.getDBResultAsBeanList(sql, ManifestationIdentifierObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(String manifestionId) {
        Log.info("verifying manifestation identifier... "+this.identifier);

        if(this.identifier!=null) {
            getManifestationIdentifierByID(this.identifier,manifestionId);
            Assert.assertEquals(getBreadcrumbMessage()+">identifier type",this.identifierType.get("code"), manifestationIDentifiersDO.get(0).getF_type());
            Log.info("verified...identifier type");
            if(!(this.effectiveStartDate==null && manifestationIDentifiersDO.get(0).getEffective_start_date()==null))
                Assert.assertEquals( getBreadcrumbMessage()+">effective start date", this.effectiveStartDate,manifestationIDentifiersDO.get(0).getEffective_start_date());
            Log.info("verified...effective start date");
            if(!(this.effectiveEndDate==null && manifestationIDentifiersDO.get(0).getEffective_end_date()==null))
                Assert.assertEquals( getBreadcrumbMessage()+">effective end date", this.effectiveEndDate,manifestationIDentifiersDO.get(0).getEffective_end_date());
            Log.info("verified...effective end date");
        }
    }

}