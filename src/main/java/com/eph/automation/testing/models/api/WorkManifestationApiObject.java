package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant to fix search API v2
 * updated by Nishant @ 13 Apr 2020 to adjust data model changes for search API
 */


import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.api.ManifestationProductAPIObject;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.util.TypeKey;
import com.google.common.base.Joiner;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkManifestationApiObject {
    public WorkManifestationApiObject() {}
    private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private manifestationcore manifestationCore;
    public manifestationcore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(manifestationcore manifestationCore) {this.manifestationCore = manifestationCore; }

    public class manifestationcore {
        private String keyTitle;
        public String getKeyTitle() {return keyTitle;}
        public void setKeyTitle(String keyTitle) {this.keyTitle = keyTitle;}

        private Boolean internationalEditionInd;
        public Boolean getInternationalEditionInd() {return internationalEditionInd;}
        public void setInternationalEditionInd(Boolean internationalEditionInd) {this.internationalEditionInd = internationalEditionInd;}

        private String firstPubDate;
        public String getFirstPubDate() {return firstPubDate;}
        public void setFirstPubDate(String firstPubDate) {this.firstPubDate = firstPubDate;}

        private List<ManifestationIdentifiersApiObject> identifiers;
        public List<ManifestationIdentifiersApiObject> getIdentifiers() {return identifiers;}
        public void setIdentifiers(List<ManifestationIdentifiersApiObject> identifiers) {this.identifiers = identifiers;}

        private HashMap<String, Object> type;
        public HashMap<String, Object> getType() {return type;}
        public void setType(HashMap<String, Object> type) {this.type = type;}

        private HashMap<String, Object> status;
        public HashMap<String, Object> getStatus() {return status;}
        public void setStatus(HashMap<String, Object> status) {this.status = status;}

    }

        //created by Nishant @ 17 Apr 2020
        private List<ManifestationProductAPIObject> products;
        public List<ManifestationProductAPIObject> getProducts() {return products;}
        public void setProducts(List<ManifestationProductAPIObject> products) {this.products = products;}

    private void getManifestationByID(String manifestationID) {
        List<String> ids = new ArrayList<>();
        ids.add(manifestationID);
        String sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(ids));
        if(manifestationDataObjectFromEPHGD!=null){manifestationDataObjectFromEPHGD.clear();}
        manifestationDataObjectFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB(){
        Log.info("comparing below for work manifestations... "+this.id);
        getManifestationByID(this.id);

        Log.info("\n-keyTitle\n-internationalEditionInd\n-firstPubDate\n-manifestationIdentifier");
        Assert.assertEquals(manifestationCore.keyTitle, manifestationDataObjectFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
        if(!(manifestationDataObjectFromEPHGD.get(0).getInternationalEditionInd()==null)||!(manifestationCore.internationalEditionInd==null)) {
            Assert.assertEquals(Boolean.valueOf(manifestationCore.internationalEditionInd), Boolean.valueOf(manifestationDataObjectFromEPHGD.get(0).getInternationalEditionInd()));
        }
        Assert.assertEquals(manifestationCore.firstPubDate, manifestationDataObjectFromEPHGD.get(0).getFIRST_PUB_DATE());
        if(manifestationCore.identifiers!=null) {for (ManifestationIdentifiersApiObject manifestationIdentifier : manifestationCore.identifiers) {manifestationIdentifier.compareWithDB();}}
        Log.info( "\n-manifestation type\n-manifestation status");
        Assert.assertEquals(manifestationCore.type.get("code"), manifestationDataObjectFromEPHGD.get(0).getF_TYPE());
        Assert.assertEquals(manifestationCore.status.get("code"), manifestationDataObjectFromEPHGD.get(0).getF_STATUS());
        if(products!=null) {
            Log.info("comparing manifestation products...");
            for (ManifestationProductAPIObject manifestationProduct : products) {
                manifestationProduct.compareWithDB();
            }
        }


    }
}


