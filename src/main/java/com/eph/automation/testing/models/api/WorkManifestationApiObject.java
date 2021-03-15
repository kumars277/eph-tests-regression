package com.eph.automation.testing.models.api;
/*
 * Created by GVLAYKOV
 * updated by Nishant to fix search API v2
 * updated by Nishant @ 13 Apr 2020 to adjust data model changes for search API
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkManifestationApiObject {
    public WorkManifestationApiObject() {
    }

   // private List<ManifestationDataObject> manifestationDataObjectFromEPHGD;

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private ManifestationCore manifestationCore;
    public ManifestationCore getManifestationCore() {return manifestationCore;}
    public void setManifestationCore(ManifestationCore manifestationCore) {this.manifestationCore = manifestationCore;}

    //created by Nishant @ 17 Apr 2020
    private List<ManifestationProductAPIObject> products;
    public List<ManifestationProductAPIObject> getProducts() {return products;}
    public void setProducts(List<ManifestationProductAPIObject> products) {this.products = products;}

    private ManifestationExtended manifestationExtended;
    public ManifestationExtended getManifestationExtended() {return manifestationExtended;}
    public void setManifestationExtended(ManifestationExtended manifestationExtended) {this.manifestationExtended = manifestationExtended;}

    public void getManifestationDetailByID(String manifestationID) {
        List<String> ids = new ArrayList<>();
        ids.add(manifestationID);
        String sql = String.format(APIDataSQL.SELECT_MANIFESTATIONS_DATA_IN_EPH_GD_BY_ID, Joiner.on("','").join(ids));
        if (DataQualityContext.manifestationDataObjectsFromEPHGD != null) {
            DataQualityContext.manifestationDataObjectsFromEPHGD.clear();
        }
        DataQualityContext.manifestationDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ManifestationDataObject.class, Constants.EPH_URL);
    }

    public void compareWithDB() {
        Log.info("Verifying work manifestations... " + this.id);
        getManifestationDetailByID(this.id);
        manifestationCore.compareWithDB(this.id);

        if (manifestationExtended != null) {
            Log.info("\nverifying manifestationExtended Data...");
            getJsonToObject_extendedManifestation(this.id);
            manifestationExtended.compareWithDB();
        }

        if (products != null) {
            Log.info("verifying manifestation products");
            for (ManifestationProductAPIObject manifestationProduct : products) {
                manifestationProduct.compareWithDB();
            }
        }
    }

    public void getJsonToObject_extendedManifestation(String manifestationId) {
        //created by Nishant @ 01 Jul 2020 to verify extended json value with APIv3
        //updated by Nishant @ 09 Jul 2020 for JRBI data validation on UAT JF UI
        String sql ="";
        if(TestContext.getValues().environment=="UAT")
             sql = "SELECT \"json\" FROM ephuat_extended_data_stitch.stch_manifestation_ext_json where epr_id='" + manifestationId + "'";
        else sql = "SELECT \"json\" FROM ephsit_extended_data_stitch.stch_manifestation_ext_json where epr_id='" + manifestationId + "'";

        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        DataQualityContext.manifestationExtendedTestClass = new Gson().fromJson(jsonValue.get(0).get("json"), ManifestationExtendedTestClass.class);
    }
}


