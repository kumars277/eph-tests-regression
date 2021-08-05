package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 * updated by Nishant @ 28 Apr 2020
 * optimised by Nishant @ 13 May 2020
 * //update by Nishant @ 04Feb2021, EPHD-2747
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.eph.automation.testing.steps.ApiSearchDataCheckStitchingLayerSteps;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

import static com.eph.automation.testing.models.contexts.DataQualityContext.randomIdsData;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductApiObject {
    public ProductApiObject() {}
    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    private String createdDate;
    public String getCreatedDate() {return createdDate;}
    public void setCreatedDate(String createdDate) {this.createdDate = createdDate;}

    public String updatedDate;
    public String getUpdatedDate() {return updatedDate;}
    public void setUpdatedDate(String updatedDate) {this.updatedDate = updatedDate;}

    private String id;
    public String getId() {return id;}
    public void setId(String id) {this.id = id;}

    private ProductCore productCore;
    public ProductCore getProductCore() {return productCore;}
    public void setProductCore(ProductCore productCore) {this.productCore = productCore;}


    //added by Nishant @ 29 Jan 2021, EPHD-2747
    private AvailabilityExtendedAPIObj availabilityExtended;
    public AvailabilityExtendedAPIObj getAvailabilityExtended() {return availabilityExtended;}
    public void setAvailabilityExtended(AvailabilityExtendedAPIObj availabilityExtended) {this.availabilityExtended = availabilityExtended;}

    ////added by Nishant @ 02 Feb 2021, EPHD-2747

    private PricingExtendedAPIObj pricingExtended;
    public PricingExtendedAPIObj getPricingExtended() {return pricingExtended;}
    public void setPricingExtended(PricingExtendedAPIObj pricingExtended) {this.pricingExtended = pricingExtended;}

    private ProductManifestationApiObject manifestation;
    public ProductManifestationApiObject getManifestation() {return manifestation;}
    public void setManifestation(ProductManifestationApiObject manifestation) {this.manifestation = manifestation;}

    private WorkApiObject work;
    public WorkApiObject getWork() {return work;}
    public void setWork(WorkApiObject work) {this.work = work;}

    /*
    private work work;
    public ProductApiObject.work getWork() {return work;}
    public void setWork(ProductApiObject.work work) {this.work = work;}

    public class work{
        private String id;
        public String getId() {return id;}
        public void setId(String id) {this.id = id;}

        private workCore workCore;
        public workCore getWorkCore() {return workCore;}
        public void setWorkCore(workCore workCore) {this.workCore = workCore;}


        }
        */

    public void compareWithDB() {
       try {
           getProductDataFromEPHGD(this.id);
           createdDate = createdDate.replace("T", " ").replace("Z", "");
           updatedDate = updatedDate.replace("T", " ").replace("Z", "");


           Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - ", createdDate.substring(0, 21), this.productDataObjectsFromEPHGD.get(0).getCREATED().substring(0, 21));
           // Assert.assertEquals(DataQualityContext.breadcrumbMessage + " - ", updatedDate,this.productDataObjectsFromEPHGD.get(0).getUPDATED());

           //1. ProductCore comparison
           productCore.compareWithDB(this.id);

           //2. availabilityExtended comparison
           if (availabilityExtended != null) {//created by Nishant @ 8 Mar 2021 EPHD-2898
               Log.info("---- verifying product availabilityExtended data...");
               ApiSearchDataCheckStitchingLayerSteps apiSearchDataCheckStitchingLayer = new ApiSearchDataCheckStitchingLayerSteps();
               String sql;
               if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
                   sql = "select epr_id,  \"json\" from ephuat_extended_data_stitch.stch_product_ext_json where extension_type = 'Availability' and epr_id='" + id + "'";
               else
                   sql = "select epr_id,  \"json\" from ephsit_extended_data_stitch.stch_product_ext_json where extension_type = 'Availability' and epr_id='" + id + "'";

               randomIdsData = DBManager.getDBResultMap(sql, Constants.EPH_URL);
               AvailabilityExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(0).get("json").toString(), AvailabilityExtendedTestClass.class);
               apiSearchDataCheckStitchingLayer.compare_stch_product_ext_json_byAvailability(jsonValue, availabilityExtended);
           }

           //3. pricingExtended comparison
           if (pricingExtended != null) {//created by Nishant @ 8 Mar 2021 EPHD-2898
               Log.info("---- verifying product pricingExtended data...");
               ApiSearchDataCheckStitchingLayerSteps apiSearchDataCheckStitchingLayer = new ApiSearchDataCheckStitchingLayerSteps();

               String dbEnv = "uat";
               if (TestContext.getValues().environment.equalsIgnoreCase("SIT"))
                    dbEnv = "sit";
               else if (TestContext.getValues().environment.equalsIgnoreCase("UAT"))
                   dbEnv = "uat";

               String sql = "select  epr_id ,json from eph"+dbEnv+"_extended_data_stitch.stch_product_ext_json where extension_type = 'Prices' and epr_id='" + id + "'";

               randomIdsData = DBManager.getDBResultMap(sql, Constants.EPH_URL);
               PricingExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(0).get("json").toString(), PricingExtendedTestClass.class);
               apiSearchDataCheckStitchingLayer.compare_stch_product_ext_json_byPricing(jsonValue, pricingExtended);
           }

           //4. pricingExtended comparison
           if (manifestation != null) {
               manifestation.compareWithDB();
           }

           //5. pricingExtended comparison
           //implemented by Nishant @ 8 May 2020
           if (work != null) {
               DataQualityContext.breadcrumbMessage += "->" + work.getId();
               work.getWorkCore().compareWithDB(work.getId());
               if (work.getWorkExtended() != null) {
                   WorkApiObject workApiObject = new WorkApiObject();
                   workApiObject.getJsonToObject_extendedWork(work.getId());
                   work.getWorkExtended().compareWithDB(work.getId());
               }
           }
       }
       catch (Exception e)
       {
           e.getMessage();

           Assert.assertFalse(DataQualityContext.breadcrumbMessage +" e.message>"+e.getMessage()+ " scenario Failed ", true);
        //   DataQualityContext.api_response.prettyPrint();
       }

    }

    private void getProductDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.GET_GD_DATA_PRODUCT, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager.getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    public void compareAvailabilityJsonValue()
    {
        availabilityExtended.compareApplications();
    }
}