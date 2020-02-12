package com.eph.automation.testing.models.api;
/**
 * Created by GVLAYKOV
 */
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductApiObject {
    public ProductApiObject() {
    }
    private static List<ProductDataObject> productDataObjectsFromEPHGD;

    private String id;
    private String name;
    private String shortName;
    private Boolean separatelySaleableInd;
    private Boolean trialAllowedInd;
    private String launchDate;
    private HashMap<String,Object> type;
    private HashMap<String,Object> status;
    private PricesApiObject[] prices;
    private HashMap<String,Object> revenueModel;
    private PersonsApiObject[] persons;
    private WorkApiObject work;
    public ProductManifestationApiObject manifestation;

    public HashMap<String, Object> getEtaxProductCode() {
        return etaxProductCode;
    }

    public void setEtaxProductCode(HashMap<String, Object> etaxProductCode) {
        this.etaxProductCode = etaxProductCode;
    }

    private HashMap<String,Object> etaxProductCode;

    public PersonsApiObject[] getPersons() {
        return persons;
    }

    public void setPersons(PersonsApiObject[] persons) {
        this.persons = persons;
    }

    public WorkApiObject getWork() {
        return work;
    }

    public void setWork(WorkApiObject work) {
        this.work = work;
    }

    public void compareWithDB(){
        getProductDataFromEPHGD(this.id);
        Assert.assertEquals(this.name, this.productDataObjectsFromEPHGD.get(0).getPRODUCT_NAME());
        Assert.assertEquals(this.shortName, this.productDataObjectsFromEPHGD.get(0).getPRODUCT_SHORT_NAME());
        Assert.assertEquals(this.separatelySaleableInd, this.productDataObjectsFromEPHGD.get(0).getBoolSEPARATELY_SALEABLE_IND());
        Assert.assertEquals(this.trialAllowedInd, this.productDataObjectsFromEPHGD.get(0).getBoolTRIAL_ALLOWED_IND());
        Assert.assertEquals(this.launchDate, this.productDataObjectsFromEPHGD.get(0).getFIRST_PUB_DATE());
        Assert.assertEquals(this.type.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_TYPE());
        Assert.assertEquals(this.status.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_STATUS());
        Assert.assertEquals(this.revenueModel.get("code"), this.productDataObjectsFromEPHGD.get(0).getF_REVENUE_MODEL());
        if(!(this.etaxProductCode==null)&&!(this.productDataObjectsFromEPHGD.get(0).getTAX_CODE()==null)) {
            Assert.assertEquals(this.etaxProductCode.get("code"), this.productDataObjectsFromEPHGD.get(0).getTAX_CODE());
        }
        if(!(this.persons==null)&&!(this.persons.length==0)) {
            for (PersonsApiObject person : persons) {
                person.compareWithDB_product();
            }
        }
        if(manifestation!=null) {
            manifestation.compareWithDB();
        }
    }

    private void getProductDataFromEPHGD(String workID) {
        List<String> ids = new ArrayList<>();
        ids.add(workID);
        String sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    public PricesApiObject[] getPrices() {
        return prices;
    }

    public void setPrices(PricesApiObject[] prices) {
        this.prices = prices;
    }

    public ProductManifestationApiObject getManifestationApiObject() {
        return manifestation;
    }

    public void setManifestationApiObject(ProductManifestationApiObject manifestation) {
        this.manifestation = manifestation;
    }

    public HashMap<String, Object> getStatus() {
        return status;
    }

    public void setStatus(HashMap<String, Object> status) {
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public Boolean getSeparatelySaleableInd() {
        return separatelySaleableInd;
    }

    public void setSeparatelySaleableInd(Boolean separatelySaleableInd) {
        this.separatelySaleableInd = separatelySaleableInd;
    }

    public Boolean getTrialAllowedInd() {
        return trialAllowedInd;
    }

    public void setTrialAllowedInd(Boolean trialAllowedInd) {
        this.trialAllowedInd = trialAllowedInd;
    }

    public String getLaunchDate() {
        return launchDate;
    }

    public void setLaunchDate(String launchDate) {
        this.launchDate = launchDate;
    }

    public HashMap<String, Object> getType() {
        return type;
    }

    public void setType(HashMap<String, Object> type) {
        this.type = type;
    }

    public HashMap<String, Object> getRevenueModel() {
        return revenueModel;
    }

    public void setRevenueModel(HashMap<String, Object> revenueModel) {
        this.revenueModel = revenueModel;
    }

}
