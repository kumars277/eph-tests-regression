package com.eph.automation.testing.models.api;
/**
 * Created by Nishant @ 08 Mar 2021
 */

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;
import java.util.HashMap;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductIdentifiersAPIObject {

        private List<ProductDataObject> DBproductIdentifier;

        private String identifier;
        public String getIdentifier() {return identifier;}
        public void setIdentifier(String identifier) {this.identifier = identifier;}

        private HashMap<String, Object> identifierType;
        public HashMap<String, Object> getIdentifierType() {return identifierType;}
        public void setIdentifierType(HashMap<String, Object> identifierType) {this.identifierType = identifierType;}

        private String effectiveStartDate;
        public String getEffectiveStartDate(){return effectiveStartDate;}
        public void setEffectiveStartDate(String effectiveStartDate){this.effectiveStartDate=effectiveStartDate;}


        private void getProductIdentifierByID(String productIdentifierID){
            String sql = APIDataSQL.getWorkIdentifiersDataFromGDByIdentifier.replace("PARAM1", productIdentifierID);
            DBproductIdentifier = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
        }

        public void compareWithDB(){
            getProductIdentifierByID(this.identifier);
            Log.info("verifiying work identifiers... "+this.DBproductIdentifier.get(0).getF_TYPE());
            Assert.assertEquals(this.identifierType.get("code"), this.DBproductIdentifier.get(0).getF_TYPE());
            Log.info("verified identifier ..."+this.identifier);
        }



}
