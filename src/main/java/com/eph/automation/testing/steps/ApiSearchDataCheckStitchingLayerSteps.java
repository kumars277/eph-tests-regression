package com.eph.automation.testing.steps;
//created by Nishant @ 29 Jan 2021

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.*;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.eph.automation.testing.services.db.sql.stitchingDataSQL;
import com.eph.automation.testing.steps.search_api.ApiWorksSearchSteps;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.*;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.eph.automation.testing.models.contexts.DataQualityContext.randomIdsData;
import static com.eph.automation.testing.models.contexts.DataQualityContext.ids;
import static com.eph.automation.testing.models.contexts.DataQualityContext.*;

public class ApiSearchDataCheckStitchingLayerSteps {

    private String sql;
    private APIService apiService = new APIService();
    private ApiWorksSearchSteps apiWorksSearchSteps = new ApiWorksSearchSteps();
    private ProductApiObject response;
    private WorkApiObject workResponse;
    List<Integer> ignore = new ArrayList<>();

    @Given("^we get (.*) random ids from (.*)")
    public void getRandomIds_FromStitching_Table(String noOfRandomRecords, String stc_table) {
        //created by Nishant @ 29 Jan 2021
        String dbEnv;
        if (TestContext.getValues().environment.equals("UAT")) dbEnv = "uat";
        else dbEnv = "sit";

        switch (stc_table) {
            case "stch_product_ext_json_byAvailability":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_product_ext_json_byAvailability, dbEnv, noOfRandomRecords);
                break;

            case "stch_product_ext_json_byPricing":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_product_ext_json_byPrices, dbEnv, noOfRandomRecords);
                break;

            case "stch_product_core_json":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_product_core_json, dbEnv, noOfRandomRecords);
                break;

            case "stch_manifestation_ext_json":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_manifestation_ext_json, dbEnv, noOfRandomRecords);
                break;

            case "stch_work_ext_json":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_work_ext_json, dbEnv, noOfRandomRecords);
                break;

            case "stch_work_core_json":
                sql = String.format(stitchingDataSQL.GETRandomIds_stch_work_core_json, dbEnv, noOfRandomRecords);
                break;

        }
         randomIdsData = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomIdsData.stream().map(m -> (String) m.get("epr_id")).map(String::valueOf).collect(Collectors.toList());
        Log.info("Selected random ids  : " + ids);
       // ids.clear();ids.add("EPR-W-102TSH");
        DataQualityContext.breadcrumbMessage += "->" + ids;
        Assert.assertFalse("Verify That list with random ids is not empty.", ids.isEmpty());
    }

    @And("we get stitching json data of (.*)")
    public void getStitchingDataForId(String stc_table) {
        //created by Nishant @ 29 Jan 2021
        String dbEnv;
        if (TestContext.getValues().environment.equals("UAT")) dbEnv = "uat";
        else dbEnv = "sit";

        switch (stc_table) {
            case "stch_product_ext_json_byAvailability":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_product_ext_json_byAvailability, dbEnv, Joiner.on("','").join(ids));
                break;

            case "stch_product_ext_json_byPricing":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_product_ext_json_byPrices, dbEnv, Joiner.on("','").join(ids));
                break;

            case "stch_product_core_json":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_product_core_json, dbEnv, Joiner.on("','").join(ids));
                break;

            case "stch_manifestation_ext_json":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_manifestation_ext_json, dbEnv, Joiner.on("','").join(ids));
                break;

            case "stch_work_ext_json":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_work_ext_json, dbEnv, Joiner.on("','").join(ids));
                break;

            case "stch_work_core_json":
                sql = String.format(stitchingDataSQL.getStitchingData_stch_work_core_json, dbEnv, Joiner.on("','").join(ids));
                break;

        }

        randomIdsData = DBManager.getDBResultMap(sql, Constants.EPH_URL);

    }


    @And("call PF search API for ids and compare with json for (.*)")
    public void callApiForIds(String stich_table) throws AzureOauthTokenFetchingException {
        int bound = randomIdsData.size();

            for (int i = 0; i < bound; i++) {
                Log.info(randomIdsData.get(i).get("epr_id").toString());

                switch (stich_table) {
                    case "stch_product_ext_json_byAvailability":
                        response = apiService.getProductById(randomIdsData.get(i).get("epr_id").toString());

                        AvailabilityExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(i).get("json").toString(), AvailabilityExtendedTestClass.class);
                        if (jsonValue.getAvailabilityExtended().getApplications() != null || response.getAvailabilityExtended().getApplications() != null) {
                            compare_stch_product_ext_json_byAvailability(jsonValue, response.getAvailabilityExtended());
                        }
                        break;

                    case "stch_product_ext_json_byPricing":
                        response = apiService.getProductById(randomIdsData.get(i).get("epr_id").toString());
                        PricingExtendedTestClass pricingJsonValue = new Gson().fromJson(randomIdsData.get(i).get("json").toString(), PricingExtendedTestClass.class);
                        if (pricingJsonValue.getPricingExtended().getExtendedPrices() != null || response.getPricingExtended().getExtendedPrices() != null) {
                            compare_stch_product_ext_json_byPricing(pricingJsonValue, response.getPricingExtended());
                        }
                        break;

                    case "stch_product_core_json":
                        response = apiService.getProductById(randomIdsData.get(i).get("epr_id").toString());
                        compare_stch_product_core_json(i);
                        break;

                    case "stch_manifestation_ext_json":
                        apiWorksSearchSteps.getProductDetailByManifestationId(randomIdsData.get(i).get("epr_id").toString());
                        response = apiService.getProductById(productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                        compare_stch_manifestation_ext_json(i);
                        break;

                    case "stch_work_ext_json":
                        apiWorksSearchSteps.getProductDetailByWorkId(randomIdsData.get(i).get("epr_id").toString());
                        response = apiService.getProductById(productDataObjectsFromEPHGD.get(0).getPRODUCT_ID());
                        compare_stch_work_ext_json(i);
                        break;

                    case "stch_work_core_json":
                        workResponse = apiService.getWorkByID(randomIdsData.get(i).get("epr_id").toString());
                        compare_stch_work_core_json(i);
                        break;
                        default: throw new IllegalArgumentException(stich_table);
                }
            }

    }


    public void compare_stch_product_ext_json_byAvailability(AvailabilityExtendedTestClass jsonValue, AvailabilityExtendedAPIObj apiResponseAvailabilityExtended) {
        //created by Nishant @ 01 Feb 2021, EPHD-2747
        //updated by Nishant @ 8 Jun 2021
        ArrayList<AvailabilityExtApplicationsAPIObj> jsonValue_arr = new ArrayList<>(Arrays.asList((jsonValue.getAvailabilityExtended().getApplications())));
        ArrayList<AvailabilityExtApplicationsAPIObj> apiResponse_arr = new ArrayList<>(Arrays.asList(apiResponseAvailabilityExtended.getApplications()));

        ignore.clear();
        for (int i = 0; i < apiResponse_arr.size(); i++) {
            Log.info("----->verification for application - " + i);
            boolean appFound = false;
            for (int cnt = 0; cnt < jsonValue_arr.size(); cnt++) {
                if (ignore.contains(cnt)) continue;
                if (jsonValue_arr.get(cnt).getApplicationName().equalsIgnoreCase(apiResponse_arr.get(i).getApplicationName())
                       //& jsonValue_arr.get(cnt).getAvailabilityFormat().equalsIgnoreCase(apiResponse_arr.get(i).getAvailabilityFormat())
                ) {
                    appFound = true;
                    printLog("ApplicationName");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> AvailabilityFormat", jsonValue_arr.get(cnt).getAvailabilityFormat(), apiResponse_arr.get(i).getAvailabilityFormat());
                    printLog("AvailabilityFormat");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> AvailabilityStartDate", jsonValue_arr.get(cnt).getAvailabilityStartDate(), apiResponse_arr.get(i).getAvailabilityStartDate());
                    printLog("AvailabilityStartDate");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> AvailabilityStatus", jsonValue_arr.get(cnt).getAvailabilityStatus(), apiResponse_arr.get(i).getAvailabilityStatus());
                    printLog("AvailabilityStatus");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> DeltaAnswerCodeUK", jsonValue_arr.get(cnt).getDeltaAnswerCodeUK(), apiResponse_arr.get(i).getDeltaAnswerCodeUK());
                    printLog("DeltaAnswerCodeUK");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> DeltaAnswerCodeUS", jsonValue_arr.get(cnt).getDeltaAnswerCodeUS(), apiResponse_arr.get(i).getDeltaAnswerCodeUS());
                    printLog("DeltaAnswerCodeUS");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByAvailability -> PublicationStatusANZ", jsonValue_arr.get(cnt).getPublicationStatusANZ(), apiResponse_arr.get(i).getPublicationStatusANZ());
                    printLog("PublicationStatusANZ");

                    ignore.add(cnt);
                    break;
                }
            }
            Assert.assertTrue(DataQualityContext.breadcrumbMessage + "productExtByAvailability " + i + " found in extjson", appFound);
        }

    }

    public void compare_stch_product_ext_json_byPricing(PricingExtendedTestClass jsonValue, PricingExtendedAPIObj pricingExtendedAPIObj) {
        //created by Nishant @ 02 Feb 2021, EPHD-2747

        ArrayList<PricingExtendedPricesAPIObj> jsonValue_arr = new ArrayList<>(Arrays.asList((jsonValue.getPricingExtended().getExtendedPrices())));
        ArrayList<PricingExtendedPricesAPIObj> apiResponse_arr = new ArrayList<>(Arrays.asList(pricingExtendedAPIObj.getExtendedPrices()));
        ignore.clear();
        for (int i = 0; i < apiResponse_arr.size(); i++) {
            Log.info("----->verification for extendedPrices - " + i);
            boolean priceFound = false;
            for (int cnt = 0; cnt < jsonValue_arr.size(); cnt++) {
                if (ignore.contains(cnt)) continue;
                if (jsonValue_arr.get(i).getCurrency().equalsIgnoreCase(apiResponse_arr.get(i).getCurrency())) {
                    priceFound = true;

                    printLog("Currency");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByPricing -> Amount", Float.valueOf(jsonValue_arr.get(i).getAmount()), Float.valueOf(apiResponse_arr.get(i).getAmount()));
                    printLog("Amount");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByPricing -> StartDate", jsonValue_arr.get(i).getStartDate(), apiResponse_arr.get(i).getStartDate());
                    printLog("StartDate");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "productExtByPricing -> EndDate", jsonValue_arr.get(i).getEndDate(), apiResponse_arr.get(i).getEndDate());
                    printLog("EndDate");

                    ignore.add(cnt);
                    break;
                }
            }

            Assert.assertTrue(DataQualityContext.breadcrumbMessage + "productExtByPricing " + i + " found in extjson", priceFound);
        }
    }

    private void compare_stch_product_core_json(int index) {
        //created by Nishant @ 04 Feb 2021, EPHD-2747
        ProductCoreTestClass jsonValue = new Gson().fromJson(randomIdsData.get(index).get("json").toString(), ProductCoreTestClass.class);

        //1. productCore validation
        Log.info("----->verification for productCore ");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> Name", jsonValue.getProductCore().getName(), response.getProductCore().getName());
        printLog("Name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> ShortName", jsonValue.getProductCore().getShortName(), response.getProductCore().getShortName());
        printLog("ShortName");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> SeparatelySaleableInd", jsonValue.getProductCore().getSeparatelySaleableInd(), response.getProductCore().getSeparatelySaleableInd());
        printLog("SeparatelySaleableInd");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> TrialAllowedInd", jsonValue.getProductCore().getTrialAllowedInd(), response.getProductCore().getTrialAllowedInd());
        printLog("TrialAllowedInd");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> LaunchDate", jsonValue.getProductCore().getLaunchDate(), response.getProductCore().getLaunchDate());
        printLog("LaunchDate");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> Type code", jsonValue.getProductCore().getType().get("code"), response.getProductCore().getType().get("code"));
        printLog("Type code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> Type name", jsonValue.getProductCore().getType().get("name"), response.getProductCore().getType().get("name"));
        printLog("Type name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> Status code", jsonValue.getProductCore().getStatus().get("code"), response.getProductCore().getStatus().get("code"));
        printLog("Status code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> Status Name", jsonValue.getProductCore().getStatus().get("name"), response.getProductCore().getStatus().get("name"));
        printLog("Status Name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> StatusRollUp", jsonValue.getProductCore().getStatus().get("statusRollUp"), response.getProductCore().getStatus().get("statusRollUp"));
        printLog("StatusRollUp");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> revenueModel code", jsonValue.getProductCore().getRevenueModel().get("code"), response.getProductCore().getRevenueModel().get("code"));
        printLog("revenueModel code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> revenueModel Name", jsonValue.getProductCore().getRevenueModel().get("name"), response.getProductCore().getRevenueModel().get("name"));
        printLog("revenueModel Name");
        if (jsonValue.getProductCore().getEtaxProductCode() != null || response.getProductCore().getEtaxProductCode() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> eTax code", jsonValue.getProductCore().getEtaxProductCode().get("code"), response.getProductCore().getEtaxProductCode().get("code"));
            printLog("etaxProductCode code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore -> eTax name", jsonValue.getProductCore().getEtaxProductCode().get("name"), response.getProductCore().getEtaxProductCode().get("name"));
            printLog("etaxProductCode Name");
        }

        if (jsonValue.getProductCore().getProductPersons() != null || response.getProductCore().getProductPersons() != null) {
            ArrayList<PersonsApiObject> productPersons_json = new ArrayList<>(Arrays.asList(jsonValue.getProductCore().getProductPersons()));
            ArrayList<PersonsApiObject> productPersons_api = new ArrayList<>(Arrays.asList(response.getProductCore().getProductPersons()));

            ignore.clear();
            for (int wp = 0; wp < productPersons_api.size(); wp++) {
                boolean personFound = false;
                for (int cnt = 0; cnt < productPersons_json.size(); cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if (productPersons_api.get(wp).getId().equalsIgnoreCase(productPersons_json.get(cnt).getId())) {

                        personFound = true;
                        Log.info("----->verification for productPersons " + wp);

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getId(), productPersons_json.get(cnt).getId());
                        printLog("product personID");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getRole().get("code"), productPersons_json.get(cnt).getRole().get("code"));
                        printLog("product personRole code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getRole().get("name"), productPersons_json.get(cnt).getRole().get("name"));
                        printLog("product personRole name");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getPerson().get("id"), productPersons_json.get(cnt).getPerson().get("id"));
                        printLog("product personsPersonId");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getPerson().get("firstName"), productPersons_json.get(cnt).getPerson().get("firstName"));
                        printLog("product personsPerson firstName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getPerson().get("lastName"), productPersons_json.get(cnt).getPerson().get("lastName"));
                        printLog("product personsPerson lastName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getPerson().get("peoplehubId"), productPersons_json.get(cnt).getPerson().get("peoplehubId"));
                        printLog("product personsPerson peoplehubId");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getPerson().get("email"), productPersons_json.get(cnt).getPerson().get("email"));
                        printLog("product personsPerson email");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getEffectiveStartDate(), productPersons_json.get(cnt).getEffectiveStartDate());
                        printLog("productPerson Effective_start_date");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson -> ", productPersons_api.get(wp).getEffectiveEndDate(), productPersons_json.get(cnt).getEffectiveEndDate());
                        printLog("productPerson Effective_end_date");

                        ignore.add(cnt);
                        break;
                    }
                }

                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchProductCore->productPerson " + wp + " found in stitchingDB", personFound);
            }
        }


        //2.manifestationCore validation
        if (jsonValue.getManifestation() != null) {

            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore ->id ", jsonValue.getManifestation().getId(), response.getManifestation().getId());
            printLog("manifestation id");
            if (jsonValue.getManifestation().getManifestationCore() != null) {
                Log.info("----->verification for manifestationCore ");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> keytitle", jsonValue.getManifestation().getManifestationCore().getKeyTitle(), response.getManifestation().getManifestationCore().getKeyTitle());
                printLog("keytitle");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> InternationalEditionInd", jsonValue.getManifestation().getManifestationCore().getInternationalEditionInd(), response.getManifestation().getManifestationCore().getInternationalEditionInd());
                printLog("InternationalEditionInd");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> FirstPubDate", jsonValue.getManifestation().getManifestationCore().getFirstPubDate(), response.getManifestation().getManifestationCore().getFirstPubDate());
                printLog("FirstPubDate");

                if (jsonValue.getManifestation().getManifestationCore().getIdentifiers() != null || response.getManifestation().getManifestationCore().getIdentifiers() != null) {

                    ArrayList<ManifestationIdentifiersApiObject> manifestationIdentifiers_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestation().getManifestationCore().getIdentifiers()));
                    ArrayList<ManifestationIdentifiersApiObject> manifestationIdentifiers_api = new ArrayList<>(Arrays.asList(response.getManifestation().getManifestationCore().getIdentifiers()));

                    if (!manifestationIdentifiers_api.isEmpty() || !manifestationIdentifiers_json.isEmpty()) {
                        ignore.clear();
                        for (int mi = 0; mi < manifestationIdentifiers_api.size(); mi++) {
                            Log.info("----->verification for manifestationIdentifiers " + mi);

                            boolean identifierFound = false;
                            for (int cnt = 0; cnt < manifestationIdentifiers_json.size(); cnt++) {
                                if (ignore.contains(cnt)) continue;
                                if (manifestationIdentifiers_api.get(mi).getIdentifier().equalsIgnoreCase(manifestationIdentifiers_json.get(mi).getIdentifier())) {
                                    identifierFound = true;

                                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> Identifier", manifestationIdentifiers_api.get(mi).getIdentifier(), manifestationIdentifiers_json.get(cnt).getIdentifier());
                                    printLog("Identifier");
                                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> IdentifierType", manifestationIdentifiers_api.get(mi).getIdentifierType().get("code"), manifestationIdentifiers_json.get(cnt).getIdentifierType().get("code"));
                                    printLog("IdentifierType code");
                                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> IdentifierType", manifestationIdentifiers_api.get(mi).getIdentifierType().get("name"), manifestationIdentifiers_json.get(cnt).getIdentifierType().get("name"));
                                    printLog("IdentifierType name");
                                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore ->Identifier-> Effective_start_date", manifestationIdentifiers_api.get(mi).getEffectiveStartDate(), manifestationIdentifiers_json.get(cnt).getEffectiveStartDate());
                                    printLog("Effective_start_date");
                                    ignore.add(cnt);
                                    break;
                                }

                            }

                            Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> Identifier " + mi + " found in stitchingDB", identifierFound);
                        }
                    }
                }

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> Type", jsonValue.getManifestation().getManifestationCore().getType().get("code"), response.getManifestation().getManifestationCore().getType().get("code"));
                printLog("manifestation Type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> Type", jsonValue.getManifestation().getManifestationCore().getType().get("name"), response.getManifestation().getManifestationCore().getType().get("name"));
                printLog("manifestation Type name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> TypeRollUp", jsonValue.getManifestation().getManifestationCore().getType().get("typeRollUp"), response.getManifestation().getManifestationCore().getType().get("typeRollUp"));
                printLog("manifestation TypeRollUp");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> status", jsonValue.getManifestation().getManifestationCore().getStatus().get("code"), response.getManifestation().getManifestationCore().getStatus().get("code"));
                printLog("manifestation status code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> status", jsonValue.getManifestation().getManifestationCore().getStatus().get("name"), response.getManifestation().getManifestationCore().getStatus().get("name"));
                printLog("manifestation status name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationCore -> statusRollUp", jsonValue.getManifestation().getManifestationCore().getStatus().get("statusRollUp"), response.getManifestation().getManifestationCore().getStatus().get("statusRollUp"));
                printLog("manifestation statusRollUp");
            }

            //3. workCore validation
            Log.info("----->verification for work data");
            if (jsonValue.getManifestation().getWork() != null || response.getManifestation().getWork() != null) {
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getId(), response.getManifestation().getWork().getId());
                printLog("work id");
                Log.info("----->verification for workCore ");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getTitle(), response.getManifestation().getWork().getWorkCore().getTitle());
                printLog(" work title");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getElectronicRightsInd(), response.getManifestation().getWork().getWorkCore().getElectronicRightsInd());
                printLog(" work ElectronicRightsInd");
                if (jsonValue.getManifestation().getWork().getWorkCore().getLanguage() != null || response.getManifestation().getWork().getWorkCore().getLanguage() != null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getLanguage().get("code"), response.getManifestation().getWork().getWorkCore().getLanguage().get("code"));
                    printLog(" work language code");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getLanguage().get("name"), response.getManifestation().getWork().getWorkCore().getLanguage().get("name"));
                    printLog(" work language name");
                }
                if (jsonValue.getManifestation().getWork().getWorkCore().getSubscriptionType() != null || response.getManifestation().getWork().getWorkCore().getSubscriptionType() != null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getSubscriptionType().get("code"), response.getManifestation().getWork().getWorkCore().getSubscriptionType().get("code"));
                    printLog(" work subscriptionType code");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getSubscriptionType().get("name"), response.getManifestation().getWork().getWorkCore().getSubscriptionType().get("name"));
                    printLog(" work subscriptionType name");
                }

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getEditionNumber(), response.getManifestation().getWork().getWorkCore().getEditionNumber());
                printLog(" work editionNumber");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getCopyrightYear(), response.getManifestation().getWork().getWorkCore().getCopyrightYear());
                printLog(" work copyrightYear");
                ArrayList<WorkIdentifiersApiObject> workIdentifiers_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestation().getWork().getWorkCore().getIdentifiers()));
                ArrayList<WorkIdentifiersApiObject> workIdentifiers_api = new ArrayList<>(Arrays.asList(response.getManifestation().getWork().getWorkCore().getIdentifiers()));
                if (!workIdentifiers_api.isEmpty() || !workIdentifiers_json.isEmpty()) {

                    ignore.clear();
                    for (int wi = 0; wi < workIdentifiers_api.size(); wi++) {

                        boolean identifierFound = false;
                        for (int cnt = 0; cnt < workIdentifiers_json.size(); cnt++) {
                            if (ignore.contains(cnt)) continue;
                            if (workIdentifiers_api.get(wi).getIdentifier().equalsIgnoreCase(workIdentifiers_json.get(cnt).getIdentifier())) {
                                identifierFound = true;
                                Log.info("----->verification for workIdentifiers " + wi);
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workIdentifiers_api.get(wi).getIdentifier(), workIdentifiers_json.get(cnt).getIdentifier());
                                printLog("work identifier");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workIdentifiers_api.get(wi).getIdentifierType().get("code"), workIdentifiers_json.get(cnt).getIdentifierType().get("code"));
                                printLog("work identifierType code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workIdentifiers_api.get(wi).getIdentifierType().get("name"), workIdentifiers_json.get(cnt).getIdentifierType().get("name"));
                                printLog("work identifierType name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workIdentifiers_api.get(wi).getEffectiveStartDate(), workIdentifiers_json.get(cnt).getEffectiveStartDate());
                                printLog("work effectiveStartDate");

                                ignore.add(cnt);
                                break;
                            }
                        }

                        Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchManifestation -> work->Identifier " + wi + " found in stitchingDB", identifierFound);
                    }
                }

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getType().get("code"), response.getManifestation().getWork().getWorkCore().getType().get("code"));
                printLog("work Type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getType().get("name"), response.getManifestation().getWork().getWorkCore().getType().get("name"));
                printLog("work Type name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getType().get("typeRollUp"), response.getManifestation().getWork().getWorkCore().getType().get("typeRollUp"));
                printLog("work TypeRollUp");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getStatus().get("code"), response.getManifestation().getWork().getWorkCore().getStatus().get("code"));
                printLog("work status code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getStatus().get("name"), response.getManifestation().getWork().getWorkCore().getStatus().get("name"));
                printLog("work status name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getStatus().get("statusRollUp"), response.getManifestation().getWork().getWorkCore().getStatus().get("statusRollUp"));
                printLog("work statusRollUp");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getImprint().get("code"), response.getManifestation().getWork().getWorkCore().getImprint().get("code"));
                printLog("work imprint code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getImprint().get("name"), response.getManifestation().getWork().getWorkCore().getImprint().get("name"));
                printLog("work imprint name");

                if (jsonValue.getManifestation().getWork().getWorkCore().getSocietyOwnership() != null || response.getManifestation().getWork().getWorkCore().getSocietyOwnership() != null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("code"), response.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("code"));
                    printLog("work societyOwnership code");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("name"), response.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("name"));
                    printLog("work societyOwnership name");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("ownershipRollUp"), response.getManifestation().getWork().getWorkCore().getSocietyOwnership().get("ownershipRollUp"));
                    printLog("work societyOwnership ownershipRollUp");
                }

                if (jsonValue.getManifestation().getWork().getWorkCore().getLegalOwnership() != null ||
                        response.getManifestation().getWork().getWorkCore().getLegalOwnership() != null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getLegalOwnership().get("code"), response.getManifestation().getWork().getWorkCore().getLegalOwnership().get("code"));
                    printLog("work legalOwnership code");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getLegalOwnership().get("name"), response.getManifestation().getWork().getWorkCore().getLegalOwnership().get("name"));
                    printLog("work legalOwnership name");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getLegalOwnership().get("ownershipRollUp"), response.getManifestation().getWork().getWorkCore().getLegalOwnership().get("ownershipRollUp"));
                    printLog("work legalOwnership ownershipRollUp");
                }
  /*              Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getOpenAccessType().get("code"), response.getManifestation().getWork().getWorkCore().getOpenAccessType().get("code"));
                printLog("work openAccessType code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getOpenAccessType().get("name"), response.getManifestation().getWork().getWorkCore().getOpenAccessType().get("name"));
                printLog("work openAccessType name");
*/
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getPmc().getCode(), response.getManifestation().getWork().getWorkCore().getPmc().getCode());
                printLog("work pmc code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getPmc().getName(), response.getManifestation().getWork().getWorkCore().getPmc().getName());
                printLog("work pmc name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getPmc().getPmg().get("code"), response.getManifestation().getWork().getWorkCore().getPmc().getPmg().get("code"));
                printLog("work pmg code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getPmc().getPmg().get("name"), response.getManifestation().getWork().getWorkCore().getPmc().getPmg().get("name"));
                printLog("work pmg name");
                if (jsonValue.getManifestation().getWork().getWorkCore().getAccountableProduct() != null ||
                        response.getManifestation().getWork().getWorkCore().getAccountableProduct() != null) {
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductSegmentCode(), response.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductSegmentCode());
                    printLog("work accountableProduct GlProductSegmentCode");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductSegmentName(), response.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductSegmentName());
                    printLog("work accountableProduct GlProductSegmentName");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductParentValue().get("code"), response.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductParentValue().get("code"));
                    printLog("work accountableProduct GlProductParentValue Code");
                    Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", jsonValue.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductParentValue().get("name"), response.getManifestation().getWork().getWorkCore().getAccountableProduct().getGlProductParentValue().get("name"));
                    printLog("work accountableProduct GlProductParentValue Name");
                }
                if (jsonValue.getManifestation().getWork().getWorkCore().getWorkFinancialAttributes() != null || response.getManifestation().getWork().getWorkCore().getWorkFinancialAttributes() != null) {
                    ArrayList<FinancialAttributesApiObject> workFinancialAttributes_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestation().getWork().getWorkCore().getWorkFinancialAttributes()));
                    ArrayList<FinancialAttributesApiObject> workFinancialAttributes_api = new ArrayList<>(Arrays.asList(response.getManifestation().getWork().getWorkCore().getWorkFinancialAttributes()));

                    ignore.clear();
                    for (int wf = 0; wf < workFinancialAttributes_api.size(); wf++) {

                        boolean finAttributesFound = false;
                        for (int cnt = 0; cnt < workFinancialAttributes_json.size(); cnt++) {
                            if (ignore.contains(cnt)) continue;
                            if (workFinancialAttributes_api.get(wf).getGlCompany().get("code").toString().equalsIgnoreCase(workFinancialAttributes_json.get(cnt).getGlCompany().get("code").toString())) {
                                finAttributesFound = true;
                                Log.info("----->verification for workFinancialAttributes " + wf);
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getGlCompany().get("code"), workFinancialAttributes_json.get(cnt).getGlCompany().get("code"));
                                printLog("work getGlCompany code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getGlCompany().get("name"), workFinancialAttributes_json.get(cnt).getGlCompany().get("name"));
                                printLog("work getGlCompany name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getCostResponsibilityCentre().get("code"), workFinancialAttributes_json.get(cnt).getCostResponsibilityCentre().get("code"));
                                printLog("work getCostResponsibilityCentre code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getCostResponsibilityCentre().get("name"), workFinancialAttributes_json.get(cnt).getCostResponsibilityCentre().get("name"));
                                printLog("work getCostResponsibilityCentre name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getRevenueResponsibilityCentre().get("code"), workFinancialAttributes_json.get(cnt).getRevenueResponsibilityCentre().get("code"));
                                printLog("work getRevenueResponsibilityCentre code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getRevenueResponsibilityCentre().get("name"), workFinancialAttributes_json.get(cnt).getRevenueResponsibilityCentre().get("name"));
                                printLog("work getRevenueResponsibilityCentre name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->Work -> ", workFinancialAttributes_api.get(wf).getEffectiveStartDate(), workFinancialAttributes_json.get(cnt).getEffectiveStartDate());
                                printLog("work Effective_start_date");

                                ignore.add(cnt);
                                break;
                            }
                        }

                        Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchManifestation -> workFinancialAttributes " + wf + " found in stitchingDB", finAttributesFound);
                    }
                }

                ArrayList<PersonsApiObject> workPersons_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestation().getWork().getWorkCore().getWorkPersons()));
                ArrayList<PersonsApiObject> workPersons_api = new ArrayList<>(Arrays.asList(response.getManifestation().getWork().getWorkCore().getWorkPersons()));
                if (!workPersons_api.isEmpty() || !workPersons_json.isEmpty()) {

                    ignore.clear();
                    for (int wp = 0; wp < workPersons_api.size(); wp++) {

                        boolean workPersonFound = false;
                        for (int cnt = 0; cnt < workPersons_json.size(); cnt++) {
                            if (ignore.contains(cnt)) continue;
                            if (workPersons_api.get(wp).getId().equalsIgnoreCase(workPersons_json.get(cnt).getId())) {
                                workPersonFound = true;
                                Log.info("----->verification for workPersons " + wp);
                                printLog("work personID");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" personRole code -> ", workPersons_api.get(wp).getRole().get("code"), workPersons_json.get(cnt).getRole().get("code"));
                                printLog("work personRole code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" personRole name -> ", workPersons_api.get(wp).getRole().get("name"), workPersons_json.get(cnt).getRole().get("name"));
                                printLog("work personRole name");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" personsPersonId -> ", workPersons_api.get(wp).getPerson().get("id"), workPersons_json.get(cnt).getPerson().get("id"));
                                printLog("work personsPersonId");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" firstName -> ", workPersons_api.get(wp).getPerson().get("firstName"), workPersons_json.get(cnt).getPerson().get("firstName"));
                                printLog("work personsPerson firstName");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" lastName -> ", workPersons_api.get(wp).getPerson().get("lastName"), workPersons_json.get(cnt).getPerson().get("lastName"));
                                printLog("work personsPerson lastName");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" peoplehubId -> ", workPersons_api.get(wp).getPerson().get("peoplehubId"), workPersons_json.get(cnt).getPerson().get("peoplehubId"));
                                printLog("work personsPerson peoplehubId");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->worksPerson "+wp+" email -> ", workPersons_api.get(wp).getPerson().get("email"), workPersons_json.get(cnt).getPerson().get("email"));
                                printLog("work personsPerson email");

                                //commented by Nishant @ 8 June 2021 untill Ekin/Ron replies for startDate mismatch issue
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" Effective_start_date -> ", workPersons_api.get(wp).getEffectiveStartDate(), workPersons_json.get(cnt).getEffectiveStartDate());
                                printLog("workPerson Effective_start_date");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestation->workCore->workPerson "+wp+" Effective_end_date -> ", workPersons_api.get(wp).getEffectiveEndDate(), workPersons_json.get(cnt).getEffectiveEndDate());
                                printLog("workPerson Effective_end_date");

                                ignore.add(cnt);
                                break;
                            }
                        }
                        Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchManifestation -> work->person " + wp + " found in stitchingDB", workPersonFound);

                    }
                }

            }

        }

        Log.info("");
    }

    private void compare_stch_manifestation_ext_json(int index) {
        //created by Nishant @ 05 Feb 2021, EPHD-2747
        Log.info("----->verification for manifestationExtended ");
        ManifestationExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(index).get("json").toString(), ManifestationExtendedTestClass.class);

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getUkTextbookInd(), response.getManifestation().getManifestationExtended().getUkTextbookInd());
        printLog("UkTextbookInd");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getUsTextbookInd(), response.getManifestation().getManifestationExtended().getUsTextbookInd());
        printLog("UsTextbookInd");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getManifestationTrimText(), response.getManifestation().getManifestationExtended().getManifestationTrimText());
        printLog("ManifestationTrimText");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getCommodityCode(), response.getManifestation().getManifestationExtended().getCommodityCode());
        printLog("CommodityCode");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getDiscountCodeEMEA(), response.getManifestation().getManifestationExtended().getDiscountCodeEMEA());
        printLog("DiscountCodeEMEA");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", jsonValue.getManifestationExtended().getDiscountCodeUS(), response.getManifestation().getManifestationExtended().getDiscountCodeUS());
        printLog("DiscountCodeUS");

        if (jsonValue.getManifestationExtended().getManifestationExtendedPageCounts() != null || response.getManifestation().getManifestationExtended().getManifestationExtendedPageCounts() != null) {
            ArrayList<ManifestationExtendedPageCountsAPIObj> ManifestationExtendedPageCounts_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestationExtended().getManifestationExtendedPageCounts()));
            ArrayList<ManifestationExtendedPageCountsAPIObj> ManifestationExtendedPageCounts_api = new ArrayList<>(Arrays.asList(response.getManifestation().getManifestationExtended().getManifestationExtendedPageCounts()));

            ignore.clear();
            for (int mi = 0; mi < ManifestationExtendedPageCounts_api.size(); mi++) {
                boolean pageCountFound = false;
                for(int cnt =0;cnt<ManifestationExtendedPageCounts_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(ManifestationExtendedPageCounts_api.get(mi).getExtendedPageCount().getType().get("code").toString().equalsIgnoreCase(ManifestationExtendedPageCounts_json.get(cnt).getExtendedPageCount().getType().get("code").toString())){
                        pageCountFound = true;
                        Log.info("----->verification for manifestationExtendedPageCounts " + mi);

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", ManifestationExtendedPageCounts_api.get(mi).getExtendedPageCount().getType().get("code"), ManifestationExtendedPageCounts_json.get(cnt).getExtendedPageCount().getType().get("code"));
                        printLog("ManifestationExtendedPageCounts type code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", ManifestationExtendedPageCounts_api.get(mi).getExtendedPageCount().getType().get("name"), ManifestationExtendedPageCounts_json.get(cnt).getExtendedPageCount().getType().get("name"));
                        printLog("ManifestationExtendedPageCounts type name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", ManifestationExtendedPageCounts_api.get(mi).getExtendedPageCount().getCount(), ManifestationExtendedPageCounts_json.get(cnt).getExtendedPageCount().getCount());
                        printLog("stchManifestationExtended count");

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchManifestationExtended -> stchManifestationExtended "+mi+" found in stitchingDB",pageCountFound);
            }
        }

        if (jsonValue.getManifestationExtended().getManifestationExtendedRestrictions() != null || response.getManifestation().getManifestationExtended().getManifestationExtendedRestrictions() != null) {
            ArrayList<ManifestationExtended.ManifestationExtendedRestrictions> ManifestationExtendedRestrictions_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestationExtended().getManifestationExtendedRestrictions()));
            ArrayList<ManifestationExtended.ManifestationExtendedRestrictions> ManifestationExtendedRestrictions_api = new ArrayList<>(Arrays.asList(response.getManifestation().getManifestationExtended().getManifestationExtendedRestrictions()));

            for (int mr = 0; mr < ManifestationExtendedRestrictions_api.size(); mr++) {
                Log.info("----->verification for manifestationExtendedRestrictions " + mr);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", ManifestationExtendedRestrictions_api.get(mr).getExtendedRestriction().getType().get("code"), ManifestationExtendedRestrictions_json.get(mr).getExtendedRestriction().getType().get("code"));
                printLog("extendedRestriction type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchManifestationExtended-> ", ManifestationExtendedRestrictions_api.get(mr).getExtendedRestriction().getType().get("name"), ManifestationExtendedRestrictions_json.get(mr).getExtendedRestriction().getType().get("name"));
                printLog("extendedRestriction type name");

            }
        }

    }

    private void compare_stch_work_ext_json(int index) {
        //created by Nishant @08 Feb 2021, EPHD-2747
        Log.info("verification for workExtended");
        WorkExtendedTestClass jsonValue = new Gson().fromJson(randomIdsData.get(index).get("json").toString(), WorkExtendedTestClass.class);
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getJournalElsComInd(), response.getWork().getWorkExtended().getJournalElsComInd());
        printLog("JournalElsComInd");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getJournalAimsScope(), response.getWork().getWorkExtended().getJournalAimsScope());
        printLog("JournalAimsScope");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getJournalProdSiteCode(), response.getWork().getWorkExtended().getJournalProdSiteCode());
        printLog("JournalProdSiteCode");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getImageFileRef(), response.getWork().getWorkExtended().getImageFileRef());
        printLog("ImageFileRef");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getInternalElsDiv(), response.getWork().getWorkExtended().getInternalElsDiv());
        printLog("InternalElsDiv");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getPrimarySiteSystem(), response.getWork().getWorkExtended().getPrimarySiteSystem());
        printLog("PrimarySiteSystem");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getPrimarySiteAcronym(), response.getWork().getWorkExtended().getPrimarySiteAcronym());
        printLog("PrimarySiteAcronym");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getPrimarySiteSupportLevel(), response.getWork().getWorkExtended().getPrimarySiteSupportLevel());
        printLog("PrimarySiteSupportLevel");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getIssueProdTypeCode(), response.getWork().getWorkExtended().getIssueProdTypeCode());
        printLog("IssueProdTypeCode");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getCatalogueVolumesQty(), response.getWork().getWorkExtended().getCatalogueVolumesQty());
        printLog("CatalogueVolumesQty");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getCatalogueIssuesQty(), response.getWork().getWorkExtended().getCatalogueIssuesQty());
        printLog("CatalogueIssuesQty");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getCatalogueVolumeFrom(), response.getWork().getWorkExtended().getCatalogueVolumeFrom());
        printLog("CatalogueVolumeFrom");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getCatalogueVolumeTo(), response.getWork().getWorkExtended().getCatalogueVolumeTo());
        printLog("CatalogueVolumeTo");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getRfIssuesQty(), response.getWork().getWorkExtended().getRfIssuesQty());
        printLog("RfIssuesQty");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getRfTotalPagesQty(), response.getWork().getWorkExtended().getRfTotalPagesQty());
        printLog("RfTotalPagesQty");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getRfFvi(), response.getWork().getWorkExtended().getRfFvi());
        printLog("RfFvi");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getRfLvi(), response.getWork().getWorkExtended().getRfLvi());
        printLog("RfLvi");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", jsonValue.getWorkExtended().getPtsBusinessUnitDesc(), response.getWork().getWorkExtended().getPtsBusinessUnitDesc());
        printLog("PtsBusinessUnitDesc");

        //by Nishant @ 09 Feb 2021
        if (jsonValue.getWorkExtended().getWorkExtendedPersons() != null ||
                response.getWork().getWorkExtended().getWorkExtendedPersons() != null) {
            ArrayList<WorkExtendedPersons> workExtendedPersons_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkExtended().getWorkExtendedPersons()));
            ArrayList<WorkExtendedPersons> workExtendedPersons_api = new ArrayList<>(Arrays.asList(response.getWork().getWorkExtended().getWorkExtendedPersons()));

            ignore.clear();
            for (int wp = 0; wp < workExtendedPersons_api.size(); wp++) {
                boolean extPersonFound = false;
                for(int cnt =0;cnt<workExtendedPersons_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workExtendedPersons_json.get(cnt).getExtendedRole().get("code").toString().equalsIgnoreCase(workExtendedPersons_api.get(wp).getExtendedRole().get("code").toString())){
                        extPersonFound = true;
                        Log.info("----->verification for workExtendedPersons " + wp);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedRole().get("code"), workExtendedPersons_api.get(wp).getExtendedRole().get("code"));
                        printLog("extendedRole code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedRole().get("name"), workExtendedPersons_api.get(wp).getExtendedRole().get("name"));
                        printLog("extendedRole name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedPerson().get("firstName"), workExtendedPersons_api.get(wp).getExtendedPerson().get("firstName"));
                        printLog("extendedPerson firstName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedPerson().get("lastName"), workExtendedPersons_api.get(wp).getExtendedPerson().get("lastName"));
                        printLog("extendedPerson lastName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedPerson().get("peoplehubId"), workExtendedPersons_api.get(wp).getExtendedPerson().get("peoplehubId"));
                        printLog("extendedPerson peoplehubId");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedPersons_json.get(cnt).getExtendedPerson().get("email"), workExtendedPersons_api.get(wp).getExtendedPerson().get("email"));
                        printLog("extendedPerson email");

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkExtended -> extendedPerson "+wp+" found in stitchingDB",extPersonFound);


            }
        }

        if (jsonValue.getWorkExtended().getWorkExtendedEditorialBoard() != null || response.getWork().getWorkExtended().getWorkExtendedEditorialBoard() != null) {
            ArrayList<WorkExtended.WorkExtendedEditorialBoard> workExtendedEditorialBoards_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkExtended().getWorkExtendedEditorialBoard()));
            ArrayList<WorkExtended.WorkExtendedEditorialBoard> workExtendedEditorialBoards_api = new ArrayList<>(Arrays.asList(response.getWork().getWorkExtended().getWorkExtendedEditorialBoard()));

            ignore.clear();
            for (int wb = 0; wb < workExtendedEditorialBoards_api.size(); wb++) {
                boolean extEdBoardMember = false;
                for(int cnt =0;cnt<workExtendedEditorialBoards_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workExtendedEditorialBoards_json.get(cnt).getExtendedBoardRole().get("code").toString().equalsIgnoreCase(workExtendedEditorialBoards_api.get(wb).getExtendedBoardRole().get("code").toString()))
                    {
                        if (workExtendedEditorialBoards_json.get(cnt).getExtendedBoardMember() != null)
                        {
                            if (workExtendedEditorialBoards_json.get(cnt).getExtendedBoardMember().equals
                                    (workExtendedEditorialBoards_api.get(wb).getExtendedBoardMember()))
                            {
                                Log.info("----->verification for workExtendedEditorialBoards " + wb);
                                printLog("ExtendedBoardRole code");
                                printLog("ExtendedBoardMember firstName");
                                printLog("ExtendedBoardMember lastName");
                                printLog("ExtendedBoardMember notes");
                                printLog("ExtendedBoardMember affiliation");
                                printLog("ExtendedBoardMember imageUrl");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedBoardMember GroupNumber "+wb,
                                        workExtendedEditorialBoards_json.get(cnt).getGroupNumber(),
                                        workExtendedEditorialBoards_api.get(wb).getGroupNumber());
                                printLog("GroupNumber");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedBoardMember SequenceNumber "+wb,
                                        workExtendedEditorialBoards_json.get(cnt).getSequenceNumber(),
                                        workExtendedEditorialBoards_api.get(wb).getSequenceNumber());
                                printLog("SequenceNumber");
                                extEdBoardMember = true;
                                ignore.add(cnt);
                                break;
                            }
                        }
                        else{
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedBoardMember GroupNumber "+wb,
                                    workExtendedEditorialBoards_json.get(cnt).getGroupNumber(),
                                    workExtendedEditorialBoards_api.get(wb).getGroupNumber());
                            printLog("GroupNumber");

                            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedBoardMember SequenceNumber "+wb,
                                    workExtendedEditorialBoards_json.get(cnt).getSequenceNumber(),
                                    workExtendedEditorialBoards_api.get(wb).getSequenceNumber());
                            printLog("SequenceNumber");
                            extEdBoardMember = true;
                            ignore.add(cnt);
                            break;
                        }
                    }
                }


                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkExtended -> extBoardMember "+wb+" not found in stitchingDB",extEdBoardMember);

            }
        }

        if (jsonValue.getWorkExtended().getWorkExtendedSubjectAreas() != null || response.getWork().getWorkExtended().getWorkExtendedSubjectAreas() != null) {
            ArrayList<WorkExtended.WorkExtendedSubjectAreas> workExtendedSubjectAreas_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkExtended().getWorkExtendedSubjectAreas()));
            ArrayList<WorkExtended.WorkExtendedSubjectAreas> workExtendedSubjectAreas_api = new ArrayList<>(Arrays.asList(response.getWork().getWorkExtended().getWorkExtendedSubjectAreas()));
            ignore.clear();
            for (int sa = 0; sa < workExtendedSubjectAreas_api.size(); sa++) {
                boolean extSubAreaFound = false;
                for(int cnt =0;cnt<workExtendedSubjectAreas_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getCode().equalsIgnoreCase(workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getCode())){
                        extSubAreaFound = true;
                        Log.info("----->verification for workExtendedSubjectAreas " + sa);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedSubjectArea "+sa +"code", workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getCode(), workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getCode());
                        printLog("ExtendedSubjectArea code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedSubjectArea "+sa +"name", workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getName(), workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getName());
                        printLog("ExtendedSubjectArea name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedSubjectArea "+sa +"typeCode", workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getType().get("code"), workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getType().get("code"));
                        printLog("ExtendedSubjectArea type code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedSubjectArea "+sa +"typeName", workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getType().get("name"), workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getType().get("name"));
                        printLog("ExtendedSubjectArea type name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended->ExtendedSubjectArea "+sa +"priority", workExtendedSubjectAreas_json.get(cnt).getExtendedSubjectArea().getPriority(), workExtendedSubjectAreas_api.get(sa).getExtendedSubjectArea().getPriority());
                        printLog("ExtendedSubjectArea priority");

                        ignore.add(cnt);break;
                    }
                }

                Assert.assertTrue("subject area not found", extSubAreaFound);

            }
        }

        if (jsonValue.getWorkExtended().getWorkExtendedUrls() != null || response.getWork().getWorkExtended().getWorkExtendedUrls() != null) {
            ArrayList<WorkExtended.WorkExtendedUrls> workExtendedUrls_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkExtended().getWorkExtendedUrls()));
            ArrayList<WorkExtended.WorkExtendedUrls> workExtendedUrls_api = new ArrayList<>(Arrays.asList(response.getWork().getWorkExtended().getWorkExtendedUrls()));
            for (int eu = 0; eu < workExtendedUrls_api.size(); eu++) {
                Log.info("----->verification for workExtendedUrls " + eu);
                if (workExtendedUrls_json.get(eu).getExtendedUrl().getType().get("code").toString().equalsIgnoreCase("ESUB2")) {
                    Log.info("got it: " + workExtendedUrls_json.get(eu).getExtendedUrl().getType().get("name"));
                }
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedUrls_json.get(eu).getExtendedUrl().getType().get("code"), workExtendedUrls_api.get(eu).getExtendedUrl().getType().get("code"));
                printLog("workExtendedUrls type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedUrls_json.get(eu).getExtendedUrl().getType().get("name"), workExtendedUrls_api.get(eu).getExtendedUrl().getType().get("name"));
                printLog("workExtendedUrls type name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedUrls_json.get(eu).getExtendedUrl().getUrl(), workExtendedUrls_api.get(eu).getExtendedUrl().getUrl());
                printLog("workExtendedUrls url ");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedUrls_json.get(eu).getExtendedUrl().getUrlTitle(), workExtendedUrls_api.get(eu).getExtendedUrl().getUrlTitle());
                printLog("workExtendedUrls urlTitle");


            }
        }

        if (jsonValue.getWorkExtended().getWorkExtendedMetrics() != null || response.getWork().getWorkExtended().getWorkExtendedMetrics() != null) {
            ArrayList<WorkExtended.WorkExtendedMetrics> workExtendedMetrics_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkExtended().getWorkExtendedMetrics()));
            ArrayList<WorkExtended.WorkExtendedMetrics> workExtendedMetrics_api = new ArrayList<>(Arrays.asList(response.getWork().getWorkExtended().getWorkExtendedMetrics()));
            for (int em = 0; em < workExtendedMetrics_api.size(); em++) {
                Log.info("----->verification for workExtendedMetrics " + em);
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedMetrics_json.get(em).getExtendedMetric().getType().get("code"), workExtendedMetrics_api.get(em).getExtendedMetric().getType().get("code"));
                printLog("workExtendedMetrics type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedMetrics_json.get(em).getExtendedMetric().getType().get("name"), workExtendedMetrics_api.get(em).getExtendedMetric().getType().get("name"));
                printLog("workExtendedMetrics type name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedMetrics_json.get(em).getExtendedMetric().getMetric(), workExtendedMetrics_api.get(em).getExtendedMetric().getMetric());
                printLog("workExtendedMetrics metric ");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedMetrics_json.get(em).getExtendedMetric().getMetricYear(), workExtendedMetrics_api.get(em).getExtendedMetric().getMetricYear());
                printLog("workExtendedMetrics metricYear");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkExtended-> ", workExtendedMetrics_json.get(em).getExtendedMetric().getMetricUrl(), workExtendedMetrics_api.get(em).getExtendedMetric().getMetricUrl());
                printLog("workExtendedMetrics metricUrl");
            }
        }


    }

    private void compare_stch_work_core_json(int index) {
        //created by Nishant @16 Feb 2021, EPHD-2747
        Log.info("");
        Log.info("verification for workCore");
        WorkCoreTestClass jsonValue = new Gson().fromJson(randomIdsData.get(index).get("json").toString(), WorkCoreTestClass.class);
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getTitle(), workResponse.getWorkCore().getTitle());
        printLog("title");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getElectronicRightsInd(), workResponse.getWorkCore().getElectronicRightsInd());
        printLog("electronicRightsInd");

        if (jsonValue.getWorkCore().getLanguage() != null || workResponse.getWorkCore().getLanguage() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getLanguage().get("code"), workResponse.getWorkCore().getLanguage().get("code"));
            printLog("language code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getLanguage().get("name"), workResponse.getWorkCore().getLanguage().get("name"));
            printLog("language name");
        }

        if (jsonValue.getWorkCore().getSubscriptionType() != null || workResponse.getWorkCore().getSubscriptionType() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getSubscriptionType().get("code"), workResponse.getWorkCore().getSubscriptionType().get("code"));
            printLog("subscriptionType code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getSubscriptionType().get("name"), workResponse.getWorkCore().getSubscriptionType().get("name"));
            printLog("subscriptionType name");
        }

        if (jsonValue.getWorkCore().getIdentifiers() != null || workResponse.getWorkCore().getIdentifiers() != null) {
            ArrayList<WorkIdentifiersApiObject> identifiers_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkCore().getIdentifiers()));
            ArrayList<WorkIdentifiersApiObject> identifiers_api = new ArrayList<>(Arrays.asList(workResponse.getWorkCore().getIdentifiers()));

            ignore.clear();
            for (int wi = 0; wi < identifiers_api.size(); wi++) {
                boolean identifierFound = false;
                for(int cnt =0;cnt<identifiers_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(identifiers_json.get(cnt).getIdentifier().equalsIgnoreCase(identifiers_api.get(wi).getIdentifier())){
                        identifierFound = true;
                        Log.info("----->verification for work identifiers " + wi);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", identifiers_json.get(cnt).getIdentifier(), identifiers_api.get(wi).getIdentifier());
                        printLog("identifier");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", identifiers_json.get(cnt).getIdentifierType().get("code"), identifiers_api.get(wi).getIdentifierType().get("code"));
                        printLog("identifier code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", identifiers_json.get(cnt).getIdentifierType().get("name"), identifiers_api.get(wi).getIdentifierType().get("name"));
                        printLog("identifier name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", identifiers_json.get(cnt).getEffectiveStartDate(), identifiers_api.get(wi).getEffectiveStartDate());
                        printLog("effectiveStartDate");

                        ignore.add(cnt);break;
                    }
                }

                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkCore -> Identifier "+wi+" found in stitchingDB",identifierFound);
            }
        }

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getType().get("code"), workResponse.getWorkCore().getType().get("code"));
        printLog("work type code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getType().get("name"), workResponse.getWorkCore().getType().get("name"));
        printLog("work type name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getType().get("typeRollUp"), workResponse.getWorkCore().getType().get("typeRollUp"));
        printLog("work type typeRollUp");

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getStatus().get("code"), workResponse.getWorkCore().getStatus().get("code"));
        printLog("work status code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getStatus().get("name"), workResponse.getWorkCore().getStatus().get("name"));
        printLog("work status name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getStatus().get("statusRollUp"), workResponse.getWorkCore().getStatus().get("statusRollUp"));
        printLog("work status statusRollUp");

        if (jsonValue.getWorkCore().getImprint() != null || workResponse.getWorkCore().getImprint() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getImprint().get("code"), workResponse.getWorkCore().getImprint().get("code"));
            printLog("work imprint code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getImprint().get("name"), workResponse.getWorkCore().getImprint().get("name"));
            printLog("work imprint name");
        }

        if (jsonValue.getWorkCore().getSocietyOwnership() != null || workResponse.getWorkCore().getSocietyOwnership() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getSocietyOwnership().get("code"), workResponse.getWorkCore().getSocietyOwnership().get("code"));
            printLog("societyOwnership code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getSocietyOwnership().get("name"), workResponse.getWorkCore().getSocietyOwnership().get("name"));
            printLog("societyOwnership name");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getSocietyOwnership().get("ownershipRollUp"), workResponse.getWorkCore().getSocietyOwnership().get("ownershipRollUp"));
            printLog("societyOwnership ownershipRollUp");
        }
        if (jsonValue.getWorkCore().getOpenAccessType() != null || workResponse.getWorkCore().getOpenAccessType() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getOpenAccessType().get("code"), workResponse.getWorkCore().getOpenAccessType().get("code"));
            printLog("getOpenAccessType code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getOpenAccessType().get("name"), workResponse.getWorkCore().getOpenAccessType().get("name"));
            printLog("getOpenAccessType name");
        }

        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getPmc().getCode(), workResponse.getWorkCore().getPmc().getCode());
        printLog("pmc code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getPmc().getName(), workResponse.getWorkCore().getPmc().getName());
        printLog("pmc name");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getPmc().getPmg().get("code"), workResponse.getWorkCore().getPmc().getPmg().get("code"));
        printLog("pmg code");
        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getPmc().getPmg().get("name"), workResponse.getWorkCore().getPmc().getPmg().get("name"));
        printLog("pmg name");

        if (jsonValue.getWorkCore().getAccountableProduct() != null || workResponse.getWorkCore().getAccountableProduct() != null) {
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getAccountableProduct().getGlProductSegmentCode(), workResponse.getWorkCore().getAccountableProduct().getGlProductSegmentCode());
            printLog("work accountableProduct GlProductSegmentCode");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getAccountableProduct().getGlProductSegmentName(), workResponse.getWorkCore().getAccountableProduct().getGlProductSegmentName());
            printLog("work accountableProduct GlProductSegmentName");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getAccountableProduct().getGlProductParentValue().get("code"), workResponse.getWorkCore().getAccountableProduct().getGlProductParentValue().get("code"));
            printLog("work accountableProduct GlProductParentValue Code");
            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", jsonValue.getWorkCore().getAccountableProduct().getGlProductParentValue().get("name"), workResponse.getWorkCore().getAccountableProduct().getGlProductParentValue().get("name"));
            printLog("work accountableProduct GlProductParentValue Name");
        }
        ArrayList<FinancialAttributesApiObject> workFinancialAttributes_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkCore().getWorkFinancialAttributes()));
        ArrayList<FinancialAttributesApiObject> workFinancialAttributes_api = new ArrayList<>(Arrays.asList(workResponse.getWorkCore().getWorkFinancialAttributes()));
        if (!workFinancialAttributes_api.isEmpty()|| !workFinancialAttributes_json.isEmpty()){

            ignore.clear();
            for (int wf = 0; wf < workFinancialAttributes_api.size(); wf++) {

                boolean finAttributesFound = false;
                for(int cnt =0;cnt<workFinancialAttributes_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workFinancialAttributes_api.get(wf).getGlCompany().get("code").toString().equalsIgnoreCase(workFinancialAttributes_json.get(cnt).getGlCompany().get("code").toString())){
                        finAttributesFound = true;
                        Log.info("----->verification for workFinancialAttributes " + wf);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getGlCompany().get("code"), workFinancialAttributes_json.get(cnt).getGlCompany().get("code"));
                        printLog("work getGlCompany code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getGlCompany().get("name"), workFinancialAttributes_json.get(cnt).getGlCompany().get("name"));
                        printLog("work getGlCompany name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getCostResponsibilityCentre().get("code"), workFinancialAttributes_json.get(cnt).getCostResponsibilityCentre().get("code"));
                        printLog("work getCostResponsibilityCentre code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getCostResponsibilityCentre().get("name"), workFinancialAttributes_json.get(cnt).getCostResponsibilityCentre().get("name"));
                        printLog("work getCostResponsibilityCentre name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getRevenueResponsibilityCentre().get("code"), workFinancialAttributes_json.get(cnt).getRevenueResponsibilityCentre().get("code"));
                        printLog("work getRevenueResponsibilityCentre code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getRevenueResponsibilityCentre().get("name"), workFinancialAttributes_json.get(cnt).getRevenueResponsibilityCentre().get("name"));
                        printLog("work getRevenueResponsibilityCentre name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workFinancialAttributes_api.get(wf).getEffectiveStartDate(), workFinancialAttributes_json.get(cnt).getEffectiveStartDate());
                        printLog("work Effective_start_date");

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkCore -> financialAttributes "+wf+" found in stitchingDB",finAttributesFound);
            }
        }

        ArrayList<PersonsApiObject> workPersons_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkCore().getWorkPersons()));
        ArrayList<PersonsApiObject> workPersons_api = new ArrayList<>(Arrays.asList(workResponse.getWorkCore().getWorkPersons()));

        if (!workPersons_api.isEmpty() || !workPersons_json.isEmpty()) {

            ignore.clear();
            for (int wp = 0; wp < workPersons_api.size(); wp++) {

                boolean personFound = false;
                for(int cnt =0;cnt<workPersons_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workPersons_api.get(wp).getId().equalsIgnoreCase(workPersons_json.get(cnt).getId())){
                        personFound = true;
                        Log.info("----->verification for workPersons " + wp);

                        printLog("work personID");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personRole code", workPersons_api.get(wp).getRole().get("code"), workPersons_json.get(cnt).getRole().get("code"));
                        printLog("work personRole code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personRole name", workPersons_api.get(wp).getRole().get("name"), workPersons_json.get(cnt).getRole().get("name"));
                        printLog("work personRole name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personsPersonId", workPersons_api.get(wp).getPerson().get("id"), workPersons_json.get(cnt).getPerson().get("id"));
                        printLog("work personsPersonId");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personsPerson firstName", workPersons_api.get(wp).getPerson().get("firstName"), workPersons_json.get(cnt).getPerson().get("firstName"));
                        printLog("work personsPerson firstName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personsPerson lastName", workPersons_api.get(wp).getPerson().get("lastName"), workPersons_json.get(cnt).getPerson().get("lastName"));
                        printLog("work personsPerson lastName");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personsPerson peoplehubId", workPersons_api.get(wp).getPerson().get("peoplehubId"), workPersons_json.get(cnt).getPerson().get("peoplehubId"));
                        printLog("work personsPerson peoplehubId");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> work personsPerson email", workPersons_api.get(wp).getPerson().get("email"), workPersons_json.get(cnt).getPerson().get("email"));
                        printLog("work personsPerson email");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> workPerson Effective_start_date", workPersons_api.get(wp).getEffectiveStartDate(), workPersons_json.get(cnt).getEffectiveStartDate());
                        printLog("workPerson Effective_start_date");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> workPerson Effective_end_date", workPersons_api.get(wp).getEffectiveEndDate(), workPersons_json.get(cnt).getEffectiveEndDate());
                        printLog("workPerson Effective_end_date");


                        ignore.add(cnt);break;
                    }
                }

                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkCore -> workPerson "+wp+" found in stitchingDB",personFound);


            }
        }


        if (jsonValue.getWorkCore().getWorkSubjectAreas() != null || workResponse.getWorkCore().getWorkSubjectAreas() != null) {
            ArrayList<SubjectAreasApiObject> workCoreSubjectAreas_json = new ArrayList<>(Arrays.asList(jsonValue.getWorkCore().getWorkSubjectAreas()));
            ArrayList<SubjectAreasApiObject> workCoreSubjectAreas_api = new ArrayList<>(Arrays.asList(workResponse.getWorkCore().getWorkSubjectAreas()));

            ignore.clear();
            for (int sa = 0; sa < workCoreSubjectAreas_api.size(); sa++) {
                boolean subAreaFound = false;
                for(int cnt =0;cnt<workCoreSubjectAreas_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workCoreSubjectAreas_json.get(sa).getSubjectArea().getCode().equalsIgnoreCase(workCoreSubjectAreas_api.get(sa).getSubjectArea().getCode())){
                        subAreaFound = true;
                        Log.info("----->verification for workCore SubjectAreas " + sa);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getSubjectArea().getCode(), workCoreSubjectAreas_api.get(sa).getSubjectArea().getCode());
                        printLog("WorkCore SubjectArea code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getSubjectArea().getName(), workCoreSubjectAreas_api.get(sa).getSubjectArea().getName());
                        printLog("WorkCore SubjectArea name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getSubjectArea().getType().get("code"), workCoreSubjectAreas_api.get(sa).getSubjectArea().getType().get("code"));
                        printLog("WorkCore SubjectArea type code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getSubjectArea().getType().get("name"), workCoreSubjectAreas_api.get(sa).getSubjectArea().getType().get("name"));
                        printLog("WorkCore SubjectArea type name");

                        if(workCoreSubjectAreas_json.get(cnt).getSubjectArea().getParentSubjectArea()!=null ||
                                workCoreSubjectAreas_api.get(sa).getSubjectArea().getParentSubjectArea()!=null)
                        {
                            Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getSubjectArea().getParentSubjectArea().get("code"), workCoreSubjectAreas_api.get(sa).getSubjectArea().getParentSubjectArea().get("code"));
                            printLog("WorkCore parentSubjectArea");
                        }

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "stchWorkCore-> ", workCoreSubjectAreas_json.get(cnt).getEffectiveStartDate(), workCoreSubjectAreas_api.get(sa).getEffectiveStartDate());
                        printLog("WorkCore SubjectArea EffectiveStartDate");

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkCore -> Identifier "+sa+" found in stitchingDB",subAreaFound);
            }
        }


        //created by Nishant @ 19 Feb 2021 EPHD-2747
        if (jsonValue.getManifestations() != null || workResponse.getManifestations() != null) {
            ArrayList<WorkManifestationApiObject> workManifestation_json = new ArrayList<>(Arrays.asList(jsonValue.getManifestations()));
            ArrayList<WorkManifestationApiObject> workManifestation_api = new ArrayList<>(Arrays.asList(workResponse.getManifestations()));
           ignore.clear();
            for (int wm = 0; wm < workManifestation_api.size(); wm++) {

                boolean manifestationFound = false;
                for(int cnt =0;cnt<workManifestation_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workManifestation_json.get(wm).getId().equalsIgnoreCase(workManifestation_api.get(wm).getId())){
                        manifestationFound = true;



                Log.info("");
                Log.info("----->verification for workManifestationCore " + wm);

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getId(), workManifestation_api.get(wm).getId());
                printLog("workManifestationCore id");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getKeyTitle(), workManifestation_api.get(wm).getManifestationCore().getKeyTitle());
                printLog("workManifestationCore key title");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getInternationalEditionInd(), workManifestation_api.get(wm).getManifestationCore().getInternationalEditionInd());
                printLog("workManifestationCore internationalEditionInd ");

                ArrayList<ManifestationIdentifiersApiObject> manifestationIdentifiers_json = new ArrayList<>(Arrays.asList(workManifestation_json.get(cnt).getManifestationCore().getIdentifiers()));
                ArrayList<ManifestationIdentifiersApiObject> manifestationIdentifiers_api = new ArrayList<>(Arrays.asList(workManifestation_api.get(wm).getManifestationCore().getIdentifiers()));
                if (!manifestationIdentifiers_api.isEmpty() || !manifestationIdentifiers_json.isEmpty()) {
                    for (int mi = 0; mi < manifestationIdentifiers_api.size(); mi++) {
                        boolean identifierFound = false;
                        for(int cnt2 =0;cnt2<manifestationIdentifiers_json.size();cnt2++) {
                            if (ignore.contains(cnt)) continue;
                            if(manifestationIdentifiers_api.get(mi).getIdentifier().equalsIgnoreCase(manifestationIdentifiers_json.get(cnt2).getIdentifier())){
                                identifierFound = true;
                                Log.info("----->verification for manifestationIdentifiers " + mi);
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationIdentifiers_api.get(mi).getIdentifier(), manifestationIdentifiers_json.get(cnt2).getIdentifier());
                                printLog("Identifier");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationIdentifiers_api.get(mi).getIdentifierType().get("code"), manifestationIdentifiers_json.get(cnt2).getIdentifierType().get("code"));
                                printLog("IdentifierType code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationIdentifiers_api.get(mi).getIdentifierType().get("name"), manifestationIdentifiers_json.get(cnt2).getIdentifierType().get("name"));
                                printLog("IdentifierType name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationIdentifiers_api.get(mi).getEffectiveStartDate(), manifestationIdentifiers_json.get(cnt2).getEffectiveStartDate());
                                printLog("Effective_start_date");

                                break;
                            }
                        }
                        Assert.assertTrue(DataQualityContext.breadcrumbMessage + "workManifestationCore -> Identifier "+mi+" found in stitchingDB",identifierFound);
                    }
                }

                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getType().get("code"), workManifestation_api.get(wm).getManifestationCore().getType().get("code"));
                printLog("workManifestationCore type code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getType().get("name"), workManifestation_api.get(wm).getManifestationCore().getType().get("name"));
                printLog("workManifestationCore type name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getType().get("typeRollUp"), workManifestation_api.get(wm).getManifestationCore().getType().get("typeRollUp"));
                printLog("workManifestationCore typeRollUp");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getStatus().get("code"), workManifestation_api.get(wm).getManifestationCore().getStatus().get("code"));
                printLog("workManifestationCore getStatus code");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getStatus().get("name"), workManifestation_api.get(wm).getManifestationCore().getStatus().get("name"));
                printLog("workManifestationCore getStatus name");
                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", workManifestation_json.get(cnt).getManifestationCore().getStatus().get("statusRollUp"), workManifestation_api.get(wm).getManifestationCore().getStatus().get("statusRollUp"));
                printLog("workManifestationCore statusRollUp");


                if (workManifestation_api.get(wm).getProducts() != null || workManifestation_json.get(cnt).getProducts() != null) {
                    List<ManifestationProductAPIObject> manifestationProduct_json = workManifestation_json.get(wm).getProducts();
                    List<ManifestationProductAPIObject> manifestationProduct_api = workManifestation_api.get(wm).getProducts();
                    for (int mp = 0; mp < manifestationProduct_api.size(); mp++) {
                        boolean productFound = false;
                        for(int cnt2 =0;cnt2<manifestationProduct_json.size();cnt2++) {
                            if (ignore.contains(cnt)) continue;
                            if(manifestationProduct_api.get(mp).getId().equalsIgnoreCase(manifestationProduct_json.get(cnt2).getId())){
                                productFound = true;
                                Log.info("----->verification for manifestationProducts " + mp);
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getId(), manifestationProduct_json.get(cnt2).getId());
                                printLog("manifestationProducts id");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getName(), manifestationProduct_json.get(cnt2).getProductSummary().getName());
                                printLog("manifestationProducts name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getType().get("code"), manifestationProduct_json.get(cnt2).getProductSummary().getType().get("code"));
                                printLog("manifestationProducts type code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getType().get("name"), manifestationProduct_json.get(cnt2).getProductSummary().getType().get("name"));
                                printLog("manifestationProducts type name");

                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getStatus().get("code"), manifestationProduct_json.get(cnt2).getProductSummary().getStatus().get("code"));
                                printLog("manifestationProducts status code");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getStatus().get("name"), manifestationProduct_json.get(cnt2).getProductSummary().getStatus().get("name"));
                                printLog("manifestationProducts status name");
                                Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workManifestationCore-> ", manifestationProduct_api.get(mp).getProductSummary().getStatus().get("statusRollUp"), manifestationProduct_json.get(cnt2).getProductSummary().getStatus().get("statusRollUp"));
                                printLog("manifestationProducts status statusRollUp");

                                break;
                            }
                        }
                        Assert.assertTrue(DataQualityContext.breadcrumbMessage + "workManifestationCore -> product "+mp+" found in stitchingDB",productFound);
                    }
                }

                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "stchWorkCore->workManifestationCore -> manifestation "+wm+" found in stitchingDB",manifestationFound);
            }
        }


        ArrayList<WorkProductsApiObj> workProduct_json = new ArrayList<>(Arrays.asList(jsonValue.getProducts()));
        List<ManifestationProductAPIObject> workProducts_api = workResponse.getProducts();
        if (!workProducts_api.isEmpty() || !workProduct_json.isEmpty()) {
            ignore.clear();
            for (int mp = 0; mp < workProducts_api.size(); mp++) {
                boolean productFound = false;
                for(int cnt =0;cnt<workProduct_json.size();cnt++) {
                    if (ignore.contains(cnt)) continue;
                    if(workProducts_api.get(mp).getId().equalsIgnoreCase(workProduct_json.get(cnt).getId())){
                        productFound = true;

                        Log.info("");
                        Log.info("----->verification for workProducts " + mp);
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getId(), workProduct_json.get(cnt).getId());
                        printLog("workProducts id");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getProductSummary().getName(), workProduct_json.get(cnt).getProductSummary().getName());
                        printLog("workProducts name");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getProductSummary().getType().get("code"), workProduct_json.get(cnt).getProductSummary().getType().get("code"));
                        printLog("workProducts type code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getProductSummary().getType().get("name"), workProduct_json.get(cnt).getProductSummary().getType().get("name"));
                        printLog("workProducts type name");

                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getProductSummary().getStatus().get("code"), workProduct_json.get(cnt).getProductSummary().getStatus().get("code"));
                        printLog("workProducts status code");
                        Assert.assertEquals(DataQualityContext.breadcrumbMessage + "workProducts-> ", workProducts_api.get(mp).getProductSummary().getStatus().get("name"), workProduct_json.get(cnt).getProductSummary().getStatus().get("name"));
                        printLog("workProducts status name");
                        Assert.assertEquals(workProducts_api.get(mp).getProductSummary().getStatus().get("statusRollUp"), workProduct_json.get(cnt).getProductSummary().getStatus().get("statusRollUp"));
                        printLog("workProducts status statusRollUp");
                        ignore.add(cnt);break;
                    }
                }
                Assert.assertTrue(DataQualityContext.breadcrumbMessage + "workProducts -> "+mp+" found in stitchingDB",productFound);

            }
        }

        Log.info("");
    }


    private void printLog(String verified) {
        Log.info("verified..." + verified);
    }
}

