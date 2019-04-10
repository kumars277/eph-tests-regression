package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.api.ProductApiObject;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ProductDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.APIDataSQL;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import static com.eph.automation.testing.services.api.APIService.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Created by Georgi Vlaykov on 11/02/2019
 */
public class SearchAPISteps {

    @StaticInjection
    public DataQualityContext dataQualityContext;

    private String sql;
    private static List<String> ids;
    private ProductApiObject response;
    private WorkApiObject work_response;

    @Given("^We get (.*) random search ids for (.*)$")
    public void getRandomProductSearchManifestationIds(String numberOfRecords, String type) {
        Log.info("Get random ids ..");
        //Get property when run with jenkins
        numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        switch (type) {
            case "book":
                sql = String.format(APIDataSQL.SELECT_RANDOM_PRODUCT_IDS_FOR_SEARCH_BOOKS, numberOfRecords);
                Log.info(sql);
                break;
            default:
                break;
        }
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("PRODUCT_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product manifestationApiObject ids  : " + ids);
    }

    @Given("^We get (.*) random search id for works")
    public void getRandomWorkSearchIds(String numberOfRecords) {
        sql = String.format(APIDataSQL.SELECT_RANDOM_WORK_IDS_FOR_SEARCH, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomProductSearchIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomProductSearchIds.stream().map(m -> (String) m.get("WORK_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random product manifestationApiObject ids  : " + ids);
    }

    @And("^We get the search data from EPH GD for (.*)$")
    public void getProductsDataFromEPHGDForJournals(String type) {
        Log.info("And We get the data from EPH GD for journals ...");
        if (type.equals("book")) {
            sql = String.format(APIDataSQL.EPH_GD_PRODUCT_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        }
        Log.info(sql);

        dataQualityContext.productDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, ProductDataObject.class, Constants.EPH_URL);
    }

    @And("^We get the work search data from EPH GD$")
    public void getWorksDataFromEPHGD() {
        Log.info("And We get the data from EPH GD for journals ...");
        sql = String.format(APIDataSQL.EPH_GD_WORK_EXTRACT_FOR_SEARCH, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
    }

    @When("^the product details are retrieved and compared$")
    public void compareSearchResultsWithDB() throws IOException {
        //sort the lists before comparison
        dataQualityContext.productDataObjectsFromEPHGD.sort(Comparator.comparing(ProductDataObject::getPRODUCT_NAME));

        int bound = dataQualityContext.productDataObjectsFromEPHGD.size();
        System.out.println("Missing entries:\n");
        for (int i = 0; i < bound; i++) {
            int code = checkProductExists(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());
            if (code == 200) {
                System.out.println(i+") Checking: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());

            response = searchForProductResult(dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());


             if((dataQualityContext.productDataObjectsFromEPHGD.get(i).getBoolSEPARATELY_SALEABLE_IND()^(Boolean.valueOf(response.getSeparatelySaleableIndicator())))){
                System.out.println("\t\tAPI SeparatelySaleableIndicator: " + Boolean.valueOf(response.getSeparatelySaleableIndicator()));
                System.out.println("\t\tSIT DB SeparatelySaleableIndicator: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getBoolSEPARATELY_SALEABLE_IND());

            }

             if((dataQualityContext.productDataObjectsFromEPHGD.get(i).getBoolTRIAL_ALLOWED_IND()^(Boolean.valueOf(response.getTrialAllowedIndicator())))){
                System.out.println("\t\tAPI TrialAllowedIndicator: " + Boolean.valueOf(response.getTrialAllowedIndicator()));
                System.out.println("\t\tSIT DB TrialAllowedIndicator: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getBoolTRIAL_ALLOWED_IND());
            }

              if(!dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE().equals(response.getProductType().get("productTypeCode"))){
                  System.out.println("\t\tAPI productTypeCode: " + response.getProductType().get("productTypeCode"));
                  System.out.println("\t\tSIT DB productTypeCode: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_TYPE());
             }

              if(!dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS().equals(response.getProductStatus().get("productStatusCode"))){
                  System.out.println("\t\tAPI productStatusCode: " + response.getProductStatus().get("productStatusCode"));
                  System.out.println("\t\tSIT DB productStatusCode: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_STATUS());
              }

            if(!dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL().equals(response.getRevenueModel().get("revenueModelCode"))){
                System.out.println("\t\tAPI revenueModelCode: " + response.getRevenueModel().get("revenueModelCode"));
                System.out.println("\t\tSIT DB revenueModelCode: " + dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL());
                System.out.println(dataQualityContext.productDataObjectsFromEPHGD.get(i).getF_REVENUE_MODEL());
            }
        }else{
                System.out.println("\n\n\t\t"+ i +")ERROR: "+code+" for "+dataQualityContext.productDataObjectsFromEPHGD.get(i).getPRODUCT_ID());

            }
        }
    }


    @When("^the work details are retrieved and compared$")
    public void compareWorkSearchResultsWithDB() {
        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getWORK_ID));

        int bound = dataQualityContext.workDataObjectsFromEPHGD.size();
        System.out.println("Missing entries:\n");
        for (int i = 0; i < bound; i++) {
            int code = checkWorkExists(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            if (code == 200) {
                System.out.println(i+") Checking: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

                work_response = searchForWorkByIDResult(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());

                if(!(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE().equals(work_response.getWorkTitle()))){
                    System.out.println("\t\tAPI SeparatelySaleableIndicator: " + work_response.getWorkTitle());
                    System.out.println("\t\tSIT DB SeparatelySaleableIndicator: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE());
                }

                if(((Boolean.valueOf(dataQualityContext.workDataObjectsFromEPHGD.get(i).getELECTRONIC_RIGHTS_IND())^(Boolean.valueOf(work_response.getElectronicRightsInd()))))){
                    System.out.println("\t\tAPI TrialAllowedIndicator: " + Boolean.valueOf(work_response.getElectronicRightsInd()));
                    System.out.println("\t\tSIT DB TrialAllowedIndicator: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getELECTRONIC_RIGHTS_IND());
                }

//                if((dataQualityContext.workDataObjectsFromEPHGD.get(i).getCOPYRIGHT_YEAR().equals(work_response.getCopyrightYear()))){
//                    System.out.println("\t\tAPI TrialAllowedIndicator: " + work_response.getCopyrightYear());
//                    System.out.println("\t\tSIT DB TrialAllowedIndicator: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getCOPYRIGHT_YEAR());
//                }

                if(!dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_TYPE().equals(work_response.getWorkType().get("workTypeCode"))){
                    System.out.println("\t\tAPI productTypeCode: " + work_response.getWorkType().get("workTypeCode"));
                    System.out.println("\t\tSIT DB productTypeCode: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_TYPE());
                }

                if(!dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS().equals(work_response.getWorkStatus().get("workStatusCode"))){
                    System.out.println("\t\tAPI productStatusCode: " + work_response.getWorkStatus().get("workStatusCode"));
                    System.out.println("\t\tSIT DB productStatusCode: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS());
                }

                if(!dataQualityContext.workDataObjectsFromEPHGD.get(i).getIMPRINT().equals(work_response.getImprint().get("imprintCode"))){
                    System.out.println("\t\tAPI revenueModelCode: " + work_response.getImprint().get("imprintCode"));
                    System.out.println("\t\tSIT DB revenueModelCode: " + dataQualityContext.workDataObjectsFromEPHGD.get(i).getIMPRINT());
                }
            } else {
                System.out.println("\n\n\t\t"+ i +")ERROR: "+code+" for "+dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_ID());
            }
        }
    }
}