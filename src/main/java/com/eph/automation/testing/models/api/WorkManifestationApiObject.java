package com.eph.automation.testing.models.api;
/*
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

    private manifestationCore manifestationCore;
    public manifestationCore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(manifestationCore manifestationCore) {this.manifestationCore = manifestationCore; }

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
        Log.info("verifying work manifestations... "+this.id);
        manifestationCore.compareWithDB(this.id);

        if(products!=null) {Log.info("verifying manifestation products");
            for (ManifestationProductAPIObject manifestationProduct : products) {manifestationProduct.compareWithDB();}
        }
    }
}


