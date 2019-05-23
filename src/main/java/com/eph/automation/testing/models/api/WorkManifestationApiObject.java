package com.eph.automation.testing.models.api;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
   public class WorkManifestationApiObject {
    public WorkManifestationApiObject() {
    }
    private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;
    public void compareWithDB(){
        getManifestationsByWorkID(this.id);
    }

    private String id;
    private String keyTitle;
    private Boolean internationalEditionFlag;
    private String firstPubDate;
    private List<ManifestationIdentifiersApiObject> identifiers;
    private HashMap<String, Object> type;
    private HashMap<String, Object> status;
    private Products[] products;

    public List<ManifestationIdentifiersApiObject> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<ManifestationIdentifiersApiObject> identifiers) {
        this.identifiers = identifiers;
    }



    public Products[] getProducts() {
        return products;
    }

    public void setProducts(Products[] products) {
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

    public Boolean getInternationalEditionFlag() {
        return internationalEditionFlag;
    }

    public void setInternationalEditionFlag(Boolean internationalEditionFlag) {
        this.internationalEditionFlag = internationalEditionFlag;
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

    private void getManifestationsByWorkID(String manifestationID) {
        List<String> ids = new ArrayList<>();
        ids.add(manifestationID);
        String sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(ids));
        manifestationDataObjectFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }
}


