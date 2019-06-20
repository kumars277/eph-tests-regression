package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.TranslationContext;
import com.eph.automation.testing.models.dao.FinancialAttribsDataObject;
import com.eph.automation.testing.models.dao.TranslationsDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.FinAttrSQL;
import com.eph.automation.testing.services.db.sql.TranslationsSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class TranslationTestSteps {
    @StaticInjection
    public TranslationContext translationContext;
    public String sql;

    private String numberOfRecords;
    private static List<String> ids;
    private static List<String> workid;
    private static List<String> isbns;
    public String fWorkID;
    private static List<WorkDataObject> refreshDate;

    @Given("^We know the number of work relationship records in PMX$")
    public void getTranslationsCountDQ(){
        sql = TranslationsSQL.GET_PMX_TRANSLATIONS_COUNT;
        Log.info(sql);
        translationContext.pmxCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.PMX_URL);
        Log.info("The PMX count is: " + translationContext.pmxCount.get(0).pmxCount);
    }

    @When("^We know the work relationship records from STG$")
    public void getTranslationsCountSTG(){
        sql = TranslationsSQL.GET_STG_ALL_COUNT;
        translationContext.stgAllCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
        Log.info("The STG count is: " + translationContext.stgAllCount.get(0).stgCount);

        if (System.getProperty("LOAD") != null) {
            if(System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = TranslationsSQL.GET_STG_TRANSLATIONS_COUNT;
                translationContext.stgCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
                Log.info("The STG count is: " + translationContext.stgCount.get(0).stgCount);
            }else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = TranslationsSQL.GET_STG_TRANSLATIONS_COUNT_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                translationContext.stgCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
                Log.info("The STG count is: " + translationContext.stgCount.get(0).stgCount);
            }
        }else {
            sql = WorkCountSQL.GET_REFRESH_DATE;
            refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                    Constants.EPH_URL);

            sql = TranslationsSQL.GET_STG_TRANSLATIONS_COUNT_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            translationContext.stgCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
            Log.info("The STG count is: " + translationContext.stgCount.get(0).stgCount);
            /*sql = TranslationsSQL.GET_STG_TRANSLATIONS_COUNT;
            Log.info(sql);
            translationContext.stgCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
            Log.info("The STG count is: " + translationContext.stgCount.get(0).stgCount);
*/        }
    }

    @When("^We get the work relationship records from SA$")
    public void getTranslationsCountSA(){
        sql = TranslationsSQL.GET_SA_TRANSLATIONS_COUNT;
        translationContext.saCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
        Log.info("The SA count is: " + translationContext.saCount.get(0).saCount);
    }

    @When("^We get the work relationship records from GD$")
    public void getTranslationsCountGD(){
        sql = TranslationsSQL.GET_GD_TRANSLATIONS_COUNT;
        translationContext.gdCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + translationContext.gdCount.get(0).gdCount);
    }

    @When("^We get the work relationship records from AE$")
    public void getTranslationsCountAE(){
        sql = TranslationsSQL.GET_GD_TRANSLATIONS_COUNT;
        translationContext.gdCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + translationContext.gdCount.get(0).gdCount);

        sql = TranslationsSQL.GET_AE_TRANSLATIONS_COUNT;
        translationContext.aeCount = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
        Log.info("The AE count is: " + translationContext.aeCount.get(0).aeCount);
    }

    @Then("^The work relationship records between (.*) and (.*) are equal$")
    public void compareCount(String source, String target){
        if (source.equalsIgnoreCase("pmx")){
            Assert.assertEquals("The count between PMX and STG does not match!", translationContext.pmxCount.get(0).pmxCount,
                    translationContext.stgAllCount.get(0).stgCount);
        }else if (target.equalsIgnoreCase("GD")){
            Assert.assertEquals("The count between SA and GD does not match!", translationContext.saCount.get(0).saCount,
                    translationContext.gdCount.get(0).gdCount);
        }else if (source.equalsIgnoreCase("STG")){
            Assert.assertEquals("The count between STG and SA does not match!", translationContext.stgCount.get(0).stgCount,
                    translationContext.saCount.get(0).saCount);
        }else {
            Assert.assertEquals("The count between SA and GD + AE does not match!", translationContext.saCount.get(0).saCount,
                    translationContext.gdCount.get(0).gdCount + translationContext.aeCount.get(0).aeCount);
        }
    }

    @Given("^We have a number of translations to check$")
    public void getIDs(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = TranslationsSQL.gettingNumberOfIds.replace("PARAM1", numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("random_value")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = String.format(TranslationsSQL.gettingWorkID, Joiner.on("','").join(ids));
        List<Map<?, ?>> randomWorkID = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workid = randomWorkID.stream().map(m -> (String) m.get("work_id")).collect(Collectors.toList());
        Log.info(workid.toString());
    }

    @When("^We get the data for translations$")
    public void getTranslationData(){
        sql = String.format(TranslationsSQL.GET_PMX_TRANSLATIONS, Joiner.on("','").join(ids));
        Log.info(sql);
        translationContext.translationDataFromPMX = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.PMX_URL);

        sql = String.format(TranslationsSQL.GET_STG_Translation_DATA, Joiner.on("','").join(ids));
        Log.info(sql);
        translationContext.translationDataFromStg = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);

        sql = String.format(TranslationsSQL.GET_SA_Translation_DATA_All, Joiner.on("','").join(workid));
        translationContext.translationDataFromSAall = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);

        sql = String.format(TranslationsSQL.GET_GD_Translation_DATA, Joiner.on("','").join(workid));
        translationContext.translationDataFromGD = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
    }

    @Then("^The translations data between PMX and STG is identical$")
    public void checkTranslationData() throws ParseException {
        for (int i=0; i<translationContext.translationDataFromStg.size();i++) {
            Assert.assertEquals("The RELATIONSHIP_PMX_SOURCEREF is incorrect for id=" + ids.get(i),
                    translationContext.translationDataFromPMX.get(i).RELATIONSHIP_PMX_SOURCEREF,
                    translationContext.translationDataFromStg.get(i).RELATIONSHIP_PMX_SOURCEREF);

            Assert.assertEquals("The CHILD_PMX_SOURCE is incorrect for id=" + ids.get(i),
                    translationContext.translationDataFromPMX.get(i).CHILD_PMX_SOURCE,
                    translationContext.translationDataFromStg.get(i).CHILD_PMX_SOURCE);

            Assert.assertEquals("The PARENT_PMX_SOURCE is incorrect for id=" + ids.get(i),
                    translationContext.translationDataFromPMX.get(i).PARENT_PMX_SOURCE,
                    translationContext.translationDataFromStg.get(i).PARENT_PMX_SOURCE);

            Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + ids.get(i),
                    translationContext.translationDataFromPMX.get(i).F_RELATIONSHIP_TYPE,
                    translationContext.translationDataFromStg.get(i).F_RELATIONSHIP_TYPE);

            if (translationContext.translationDataFromPMX.get(i).EFFECTIVE_START_DATE != null
                    || translationContext.translationDataFromStg.get(i).EFFECTIVE_START_DATE != null) {
                Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + ids.get(i),
                        translationContext.translationDataFromPMX.get(i).EFFECTIVE_START_DATE.substring(0, 10),
                        translationContext.translationDataFromStg.get(i).EFFECTIVE_START_DATE);
            }
            if (translationContext.translationDataFromPMX.get(i).ENDON != null
                    || translationContext.translationDataFromStg.get(i).ENDON != null) {
                Assert.assertEquals("The ENDON is incorrect for id=" + ids.get(i),
                        translationContext.translationDataFromPMX.get(i).ENDON.substring(0,10),
                        translationContext.translationDataFromStg.get(i).ENDON);
            }
/*
            Date pmxUpdatedDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS").parse(translationContext.translationDataFromPMX.get(i).UPDATED);
            Date stgDate = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSSSSS aaa").parse(translationContext.translationDataFromStg.get(i).UPDATED);*/

            if (translationContext.translationDataFromPMX.get(i).UPDATED != null
                    ||translationContext.translationDataFromStg.get(i).UPDATED != null) {
                assertTrue("Expecting the UPDATED details from PMX and EPH Consistent for id=" + ids.get(i),
                        translationContext.translationDataFromPMX.get(i).UPDATED
                                .equals(translationContext.translationDataFromStg.get(i).UPDATED));
            }
        }
    }

    @And("^The translations data between STG and SA is identical$")
    public void checkTranslationSAData() {
        for (int i = 0; i < translationContext.translationDataFromStg.size(); i++) {
            sql = TranslationsSQL.Get_work_id.replace("PARAM1", translationContext.translationDataFromStg.get(i).PARENT_PMX_SOURCE);
            Log.info(sql);
            translationContext.workID = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
            fWorkID = translationContext.workID.get(0).workID;

            sql = TranslationsSQL.Get_child_id.replace("PARAM1", translationContext.translationDataFromStg.get(i).CHILD_PMX_SOURCE);
            Log.info(sql);
            translationContext.childID = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);

            sql = String.format(TranslationsSQL.GET_SA_Translation_DATA.replace("PARAM1", fWorkID)
                    .replace("PARAM2", translationContext.childID.get(0).workID));
            Log.info(sql);
            translationContext.translationDataFromSA = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);
            if (translationContext.translationDataFromSA.isEmpty()) {
                Log.info("There is no loaded data for Translations in SA");
            } else {
                Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + translationContext.workID.get(0).workID,
                        "WorkRelationship",
                        translationContext.translationDataFromSA.get(0).B_CLASSNAME);

                if (translationContext.translationDataFromStg.get(i).EFFECTIVE_START_DATE != null
                        || translationContext.translationDataFromSA.get(0).EFFECTIVE_START_DATE != null) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + translationContext.workID.get(0).workID,
                            translationContext.translationDataFromStg.get(i).EFFECTIVE_START_DATE,
                            translationContext.translationDataFromSA.get(0).EFFECTIVE_START_DATE);
                }
/*            sql=TranslationsSQL.Get_translation_id.replace("PARAM1", "WORK_TRANS-"+
                    translationContext.translationDataFromStg.get(i).RELATIONSHIP_PMX_SOURCEREF);
            Log.info(sql);
            translationContext.translationID = DBManager.getDBResultAsBeanList(sql, TranslationsDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The WORK_REL_TRANSLATION_ID is incorrect for id=" + translationContext.workID.get(0).workID,
                    translationContext.translationID.get(0).translationId,
                    translationContext.translationDataFromSA.get(0).WORK_REL_TRANSLATION_ID);*/


                Assert.assertEquals("The child id is incorrect for id=" + translationContext.workID.get(0).workID,
                        translationContext.childID.get(0).workID,
                        translationContext.translationDataFromSA.get(0).CHILD_PMX_SOURCE);

                if (translationContext.translationDataFromStg.get(i).ENDON != null
                        || translationContext.translationDataFromSA.get(0).ENDON != null) {
                    Assert.assertEquals("The ENDON is incorrect for id=" + translationContext.workID,
                            translationContext.translationDataFromStg.get(i).ENDON,
                            translationContext.translationDataFromSA.get(0).ENDON);
                }

                Assert.assertEquals("The relationship type is incorrect for id=" + translationContext.workID,
                        translationContext.translationDataFromStg.get(i).getF_RELATIONSHIP_TYPE(),
                        translationContext.translationDataFromSA.get(0).getF_RELATIONSHIP_TYPE());

                if (translationContext.translationDataFromStg.get(i).getRELATIONSHIP_PMX_SOURCEREF() != null
                        || translationContext.translationDataFromSA.get(0).getRELATIONSHIP_PMX_SOURCEREF() != null) {
                    Assert.assertEquals("The ENDON is incorrect for id=" + translationContext.workID,
                            translationContext.translationDataFromStg.get(i).getRELATIONSHIP_PMX_SOURCEREF(),
                            translationContext.translationDataFromSA.get(0).getRELATIONSHIP_PMX_SOURCEREF());
                }
            }
        }
    }
    @And("^The translations data between SA and GD is identical$")
    public void checkTranslationGDData() {
        if (translationContext.translationDataFromGD.isEmpty()) {
            Log.info("There is no loaded data for Translations in GD");
        } else {
            for (int i = 0; i < translationContext.translationDataFromSAall.size(); i++) {
                Assert.assertEquals("The WORK_REL_TRANSLATION_ID is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                        translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                        translationContext.translationDataFromGD.get(i).WORK_REL_TRANSLATION_ID);

                Assert.assertEquals("The RELATIONSHIP_PMX_SOURCEREF is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                        translationContext.translationDataFromSAall.get(i).RELATIONSHIP_PMX_SOURCEREF,
                        translationContext.translationDataFromGD.get(i).RELATIONSHIP_PMX_SOURCEREF);

                Assert.assertEquals("The CHILD_PMX_SOURCE is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                        translationContext.translationDataFromSAall.get(i).CHILD_PMX_SOURCE,
                        translationContext.translationDataFromGD.get(i).CHILD_PMX_SOURCE);

                Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                        translationContext.translationDataFromSAall.get(i).B_CLASSNAME,
                        translationContext.translationDataFromGD.get(i).B_CLASSNAME);

                if (translationContext.translationDataFromSAall.get(i).EFFECTIVE_START_DATE != null
                        || translationContext.translationDataFromGD.get(i).EFFECTIVE_START_DATE != null) {
                    Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                            translationContext.translationDataFromSAall.get(i).EFFECTIVE_START_DATE,
                            translationContext.translationDataFromGD.get(i).EFFECTIVE_START_DATE);
                }
                if (translationContext.translationDataFromSAall.get(i).ENDON != null
                        || translationContext.translationDataFromGD.get(i).ENDON != null) {
                    Assert.assertEquals("The ENDON is incorrect for id=" + translationContext.translationDataFromSAall.get(i).WORK_REL_TRANSLATION_ID,
                            translationContext.translationDataFromSAall.get(i).ENDON,
                            translationContext.translationDataFromGD.get(i).ENDON);
                }

                Assert.assertEquals("The relationship type is incorrect for id=" + translationContext.workID,
                        translationContext.translationDataFromSAall.get(i).getF_RELATIONSHIP_TYPE(),
                        translationContext.translationDataFromGD.get(i).getF_RELATIONSHIP_TYPE());

                if (translationContext.translationDataFromSAall.get(i).getRELATIONSHIP_PMX_SOURCEREF() != null
                        || translationContext.translationDataFromGD.get(i).getRELATIONSHIP_PMX_SOURCEREF() != null) {
                    Assert.assertEquals("The ENDON is incorrect for id=" + translationContext.workID,
                            translationContext.translationDataFromSAall.get(i).getRELATIONSHIP_PMX_SOURCEREF(),
                            translationContext.translationDataFromGD.get(i).getRELATIONSHIP_PMX_SOURCEREF());
                }
            }
        }
    }
}
