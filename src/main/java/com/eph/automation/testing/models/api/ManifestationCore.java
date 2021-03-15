package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.*;

/*
* created by Nishant @ 13 May 2020
* */
public class ManifestationCore {
   // private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;

    private String keyTitle;
    public String getKeyTitle() {return keyTitle;}
    public void setKeyTitle(String keyTitle) {this.keyTitle = keyTitle;}

    private Boolean internationalEditionInd;
    public Boolean getInternationalEditionInd() {return internationalEditionInd;}
    public void setInternationalEditionInd(Boolean internationalEditionInd) {this.internationalEditionInd = internationalEditionInd;}

    private String firstPubDate;
    public String getFirstPubDate() {return firstPubDate;}
    public void setFirstPubDate(String firstPubDate) {this.firstPubDate = firstPubDate;}

    private ManifestationIdentifiersApiObject[] identifiers;
    public ManifestationIdentifiersApiObject[] getIdentifiers() {return identifiers;}
    public void setIdentifiers(ManifestationIdentifiersApiObject[] identifiers) {this.identifiers = identifiers;}

    private HashMap<String, Object> type;
    public HashMap<String, Object> getType() {return type;}
    public void setType(HashMap<String, Object> type) {this.type = type;}

    private HashMap<String, Object> status;
    public HashMap<String, Object> getStatus() {return status;}
    public void setStatus(HashMap<String, Object> status) {this.status = status;}

    public void compareWithDB(String manifestationId) {
        Log.info("----- verifying ManifestationCore data...");
        Assert.assertEquals(keyTitle, DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());        printLog("manifestation keyTitle");


    if (DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getInternationalEditionInd() != null | internationalEditionInd != null) {
        Assert.assertEquals(Boolean.valueOf(internationalEditionInd), Boolean.valueOf(DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getInternationalEditionInd()));
        printLog("internationalEditionInd");
    }

    Assert.assertEquals(firstPubDate, DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getFIRST_PUB_DATE());    printLog("firstPubDate");

    if (identifiers != null) {
        for (ManifestationIdentifiersApiObject identif : identifiers) {
            identif.compareWithDB();
        }
        printLog("identifiers");
    }

    Assert.assertEquals(type.get("code"), DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_TYPE());
    printLog("manifestation type");

    Assert.assertEquals(status.get("code"), DataQualityContext.manifestationDataObjectsFromEPHGD.get(0).getF_STATUS());
    printLog("manifestation status code");
}
    //}

    private void getManifestationByID(String manifestationID) {
        List<String> ids = new ArrayList<>();
        ids.add(manifestationID);
        String sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(ids));
        if (DataQualityContext.manifestationDataObjectsFromEPHGD != null) {
            DataQualityContext.manifestationDataObjectsFromEPHGD.clear();
        }
        DataQualityContext.manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    private void printLog(String verified) {Log.info("verified..." + verified);}
}
