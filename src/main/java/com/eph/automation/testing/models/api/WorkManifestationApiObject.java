package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */

//updated by Nishant to fix search API v2
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
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
    public WorkManifestationApiObject() {
    }
    private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;
    public void compareWithDB(){
        getManifestationByID(this.id);
        Assert.assertEquals(this.keyTitle, manifestationDataObjectFromEPHGD.get(0).getMANIFESTATION_KEY_TITLE());
      /*  if(!(manifestationDataObjectFromEPHGD.get(0).getINTERNATIONAL_EDITION_IND()==null)||!(this.internationalEditionInd==null)) {
            Assert.assertEquals(Boolean.valueOf(this.internationalEditionInd), Boolean.valueOf(manifestationDataObjectFromEPHGD.get(0).getINTERNATIONAL_EDITION_IND()));
        }*/
        Assert.assertEquals(this.firstPubDate, manifestationDataObjectFromEPHGD.get(0).getFIRST_PUB_DATE());
        Assert.assertEquals(this.type.get("code"), manifestationDataObjectFromEPHGD.get(0).getF_TYPE());
        Assert.assertEquals(this.status.get("code"), manifestationDataObjectFromEPHGD.get(0).getF_STATUS());

    }

    private String id;
    private String keyTitle;
    private Boolean internationalEditionInd;
    private String firstPubDate;
    private List<ManifestationIdentifiersApiObject> identifiers;
    private HashMap<String, Object> type;
    private HashMap<String, Object> status;
    private List<ManifestationProductAPIObject> products;

    public List<ManifestationIdentifiersApiObject> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<ManifestationIdentifiersApiObject> identifiers) {
        this.identifiers = identifiers;
    }

    public List<ManifestationProductAPIObject> getProducts() {
        return products;
    }

    public void setProducts(List<ManifestationProductAPIObject> products) {
        this.products = products;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKeyTitle() {
        return keyTitle;
    }

    public void setKeyTitle(String keyTitle) {
        this.keyTitle = keyTitle;
    }

    public Boolean getInternationalEditionInd() {
        return internationalEditionInd;
    }

    public void setInternationalEditionInd(Boolean internationalEditionInd) {
        this.internationalEditionInd = internationalEditionInd;
    }

    public String getFirstPubDate() {
        return firstPubDate;
    }

    public void setFirstPubDate(String firstPubDate) {
        this.firstPubDate = firstPubDate;
    }

    public HashMap<String, Object> getType() {
        return type;
    }

    public void setType(HashMap<String, Object> type) {
        this.type = type;
    }

    public HashMap<String, Object> getStatus() {
        return status;
    }

    public void setStatus(HashMap<String, Object> status) {
        this.status = status;
    }


    // public void setProducts(HashMap<String, Object> products) {this.products = products; }

    /*
        static class Products {
            public Products() {
            }

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            String id;
        }
    */
    private void getManifestationByID(String manifestationID) {
        List<String> ids = new ArrayList<>();
        ids.add(manifestationID);
        String sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(ids));
        if(manifestationDataObjectFromEPHGD!=null){
            manifestationDataObjectFromEPHGD.clear();
        }
        manifestationDataObjectFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }
}


