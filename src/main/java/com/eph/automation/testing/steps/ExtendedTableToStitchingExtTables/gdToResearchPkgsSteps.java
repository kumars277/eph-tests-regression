package com.eph.automation.testing.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.PackageItemsContext;
import com.eph.automation.testing.models.dao.PkgItemsAccessObject;
import com.eph.automation.testing.services.db.dppPkgItems.gdToResearchPkgChecksSQL;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class gdToResearchPkgsSteps {

    private static String sql;
    private static List<String> ids;

    @Given("^We get the (.*) random EPR Prod ids from Semarchy tables$")
    public void getRandomprodEprIds(String numberOfRecords) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random prod ids...");
        sql = String.format(gdToResearchPkgChecksSQL.GET_RANDOM_PROD_EPR_ID, numberOfRecords);
        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.EPH_RP_URL);
        ids = randomIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
        Log.info(sql);
        Log.info(ids.toString());
    }

    @When("^Get the data from semarchy tables$")
    public void getSemarchyTableRecs() {
        Log.info("We get the Semarch Table records...");
        sql = String.format(gdToResearchPkgChecksSQL.GET_SEMARCHY_RECS, String.join("','",ids));
        PackageItemsContext.recordsFromSemarchyData = DBManager.getDBResultAsBeanList(sql, PkgItemsAccessObject.class, Constants.EPH_URL);
        Log.info(sql);
    }

    @Then("^Get the data from package item table of research package$")
    public void getRecordsFromCurrentProduct() {
        Log.info("We get the records from Current Product table...");
        sql = String.format(gdToResearchPkgChecksSQL.GET_RESEARCHPKG_PKGITEM_RECS, String.join("','",ids));
        PackageItemsContext.recordsFromPackageItem = DBManager.getDBResultAsBeanList(sql, PkgItemsAccessObject.class, Constants.EPH_RP_URL);
        Log.info(sql);
    }


    @And("^we compare the records between semarchy and research packages$")
    public void compareDataFullandCurrentProduct() {
        if (PackageItemsContext.recordsFromSemarchyData.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            Log.info("Sorting the isbn ids to compare the records between Full Load and Current Product...");
            for (int i = 0; i < PackageItemsContext.recordsFromSemarchyData.size(); i++) {

                PackageItemsContext.recordsFromSemarchyData.sort(Comparator.comparing(PkgItemsAccessObject::getproduct_id)); //sort primarykey data in the lists
                PackageItemsContext.recordsFromPackageItem.sort(Comparator.comparing(PkgItemsAccessObject::getepr_id));

                Log.info("Semrchy -> product_id = " + PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() +
                        " and Package Items -> epr_id = " + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getepr_id());
                }
                Log.info("Semrchy -> issn = " + PackageItemsContext.recordsFromSemarchyData.get(i).getissn() +
                        " and Package Items -> issn = " + PackageItemsContext.recordsFromPackageItem.get(i).getissn());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getissn(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getissn());
                }
                Log.info("Semrchy -> Journal Number = " + PackageItemsContext.recordsFromSemarchyData.get(i).getjournal_number() +
                        " and Package Items -> Journal Number = " + PackageItemsContext.recordsFromPackageItem.get(i).getjournal_number());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getjournal_number(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getjournal_number());
                }
                Log.info("Semrchy -> PMg code = " + PackageItemsContext.recordsFromSemarchyData.get(i).getf_pmg() +
                        " and Package Items -> PMg code = " + PackageItemsContext.recordsFromPackageItem.get(i).getpmg_code());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getf_pmg(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getpmg_code());
                }
                Log.info("Semrchy -> Publisher = " + PackageItemsContext.recordsFromSemarchyData.get(i).getpublisher() +
                        " and Package Items -> Publisher = " + PackageItemsContext.recordsFromPackageItem.get(i).getpublisher());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getpublisher(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getpublisher());
                }

                Log.info("Semrchy -> Publishing Director = " + PackageItemsContext.recordsFromSemarchyData.get(i).getpublishing_director() +
                        " and Package Items -> Publishing Director = " + PackageItemsContext.recordsFromPackageItem.get(i).getpublishing_director());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getpublishing_director(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getpublishing_director());
                }

                Log.info("Semrchy -> Title = " + PackageItemsContext.recordsFromSemarchyData.get(i).getwork_title() +
                        " and Package Items -> Title = " + PackageItemsContext.recordsFromPackageItem.get(i).gettitle());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getwork_title(),
                            PackageItemsContext.recordsFromPackageItem.get(i).gettitle());
                }

                Log.info("Semrchy -> Legal Ownership = " + PackageItemsContext.recordsFromSemarchyData.get(i).getlegal_ownership() +
                        " and Package Items -> Legal Ownership = " + PackageItemsContext.recordsFromPackageItem.get(i).getlegal_ownership_type());
                if (PackageItemsContext.recordsFromSemarchyData.get(i).getproduct_id() != null ||
                        (PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() != null)) {
                    Assert.assertEquals("The prod id is =" + PackageItemsContext.recordsFromPackageItem.get(i).getepr_id() + " is missing/not found in Research Pkg table",
                            PackageItemsContext.recordsFromSemarchyData.get(i).getlegal_ownership(),
                            PackageItemsContext.recordsFromPackageItem.get(i).getlegal_ownership_type());
                }

            }


        }
    }
}
