package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.MirrorsContext;
import com.eph.automation.testing.models.dao.MirrorsDataObject;
import com.eph.automation.testing.models.dao.MirrorsDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.MirrorsSQL;
import com.eph.automation.testing.services.db.sql.MirrorsSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MirrorTestSteps {
    @StaticInjection
    public MirrorsContext mirrorContext;
    public String sql;

    private String numberOfRecords;
    private static List<String> ids;
    private static List<String> workid;
    private static List<String> isbns;
    public String fWorkID;
    private static List<WorkDataObject> refreshDate;

    @Given("^We know the mirrors from STG$")
    public void getMirrorsCountSTG(){
        if (System.getProperty("LOAD") != null) {
            if(System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = MirrorsSQL.GET_STG_Mirrors_COUNT;
                Log.info(sql);
                mirrorContext.stgCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
                Log.info("The STG count is: " + mirrorContext.stgCount.get(0).stgCount);
            }else{
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = MirrorsSQL.GET_STG_Mirrors_COUNT_Updated.replace("PARAM1",refreshDate.get(1).refresh_timestamp);
                mirrorContext.stgCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
                Log.info("The STG count is: " + mirrorContext.stgCount.get(0).stgCount);
            }
        } else {
            sql = MirrorsSQL.GET_STG_Mirrors_COUNT;
            Log.info(sql);
            mirrorContext.stgCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
            Log.info("The STG count is: " + mirrorContext.stgCount.get(0).stgCount);
        }
    }

    @When("^We get the mirrors from SA$")
    public void getMirrorsCountSA(){
        sql = MirrorsSQL.GET_SA_Mirrors_COUNT;
        mirrorContext.saCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
        Log.info("The SA count is: " + mirrorContext.saCount.get(0).saCount);
    }

    @When("^We get the mirrors from GD$")
    public void getMirrorsCountGD(){
        sql = MirrorsSQL.GET_GD_Mirrors_COUNT;
        mirrorContext.gdCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + mirrorContext.gdCount.get(0).gdCount);
    }

    @When("^We get the mirrors from AE$")
    public void getMirrorsCountAE(){
        sql = MirrorsSQL.GET_GD_Mirrors_COUNT;
        mirrorContext.gdCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
        Log.info("The GD count is: " + mirrorContext.gdCount.get(0).gdCount);
        sql = MirrorsSQL.GET_AE_Mirrors_COUNT;
        mirrorContext.aeCount = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
        Log.info("The AE count is: " + mirrorContext.aeCount.get(0).aeCount);
    }

    @Then("^The mirrors between (.*) and (.*) are equal$")
    public void compareCount(String source, String target){
        if (target.equalsIgnoreCase("GD")){
            Assert.assertEquals("The count between SA and GD does not match!", mirrorContext.saCount.get(0).saCount,
                    mirrorContext.gdCount.get(0).gdCount);
        }else if (source.equalsIgnoreCase("STG")){
            Assert.assertEquals("The count between STG and SA does not match!", mirrorContext.stgCount.get(0).stgCount,
                    mirrorContext.saCount.get(0).saCount);
        }else {
            Assert.assertEquals("The count between SA and GD + AE does not match!", mirrorContext.saCount.get(0).saCount,
                    mirrorContext.gdCount.get(0).gdCount + mirrorContext.aeCount.get(0).aeCount);
        }
    }

    @Given("^We have a number of mirrors to check$")
    public void getIDs(){

        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "5";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = MirrorsSQL.gettingNumberOfIds.replace("PARAM1", numberOfRecords);
        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("random_value")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());

        sql = String.format(MirrorsSQL.gettingWorkID, Joiner.on("','").join(ids));
        Log.info(sql);
        List<Map<?, ?>> randomWorkID = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workid = randomWorkID.stream().map(m -> (String) m.get("work_id")).collect(Collectors.toList());
        Log.info(workid.toString());
    }

    @When("^We get the data for mirrors$")
    public void getmirrorData(){
        sql = String.format(MirrorsSQL.GET_PMX_Mirror, Joiner.on("','").join(ids));
        mirrorContext.mirrorDataFromPMX = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.PMX_URL);

        sql = String.format(MirrorsSQL.GET_STG_Mirror_DATA, Joiner.on("','").join(ids));
        mirrorContext.mirrorDataFromStg = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);

        sql = String.format(MirrorsSQL.GET_SA_Mirror_DATA_All, Joiner.on("','").join(workid));
        mirrorContext.mirrorDataFromSAall = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);

        sql = String.format(MirrorsSQL.GET_GD_Mirror_DATA, Joiner.on("','").join(workid));
        mirrorContext.mirrorDataFromGD = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
    }

    @Then("^The mirror data between PMX and STG is identical$")
    public void checkmirrorData(){
        for (int i=0; i<mirrorContext.mirrorDataFromPMX.size();i++) {
            Assert.assertEquals("The RELATIONSHIP_PMX_SOURCEREF is incorrect for id=" + ids.get(i),
                    mirrorContext.mirrorDataFromPMX.get(i).RELATIONSHIP_PMX_SOURCEREF,
                    mirrorContext.mirrorDataFromStg.get(i).RELATIONSHIP_PMX_SOURCEREF);

            Assert.assertEquals("The CHILD_PMX_SOURCE is incorrect for id=" + ids.get(i),
                    mirrorContext.mirrorDataFromPMX.get(i).CHILD_PMX_SOURCE,
                    mirrorContext.mirrorDataFromStg.get(i).CHILD_PMX_SOURCE);

            Assert.assertEquals("The PARENT_PMX_SOURCE is incorrect for id=" + ids.get(i),
                    mirrorContext.mirrorDataFromPMX.get(i).PARENT_PMX_SOURCE,
                    mirrorContext.mirrorDataFromStg.get(i).PARENT_PMX_SOURCE);

            Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + ids.get(i),
                    mirrorContext.mirrorDataFromPMX.get(i).F_RELATIONSHIP_TYPE,
                    mirrorContext.mirrorDataFromStg.get(i).F_RELATIONSHIP_TYPE);

            if (mirrorContext.mirrorDataFromPMX.get(i).EFFECTIVE_START_DATE != null
                    || mirrorContext.mirrorDataFromStg.get(i).EFFECTIVE_START_DATE != null) {
                Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + ids.get(i),
                        mirrorContext.mirrorDataFromPMX.get(i).EFFECTIVE_START_DATE.substring(0, 10),
                        mirrorContext.mirrorDataFromStg.get(i).EFFECTIVE_START_DATE);
            }
            if (mirrorContext.mirrorDataFromPMX.get(i).ENDON != null
                    || mirrorContext.mirrorDataFromStg.get(i).ENDON != null) {
                Assert.assertEquals("The ENDON is incorrect for id=" + ids.get(i),
                        mirrorContext.mirrorDataFromPMX.get(i).ENDON,
                        mirrorContext.mirrorDataFromStg.get(i).ENDON);
            }
        }
    }

    @And("^The mirror data between STG and SA is identical$")
    public void checkmirrorSAData(){
        for (int i=0; i<mirrorContext.mirrorDataFromStg.size();i++) {
            sql=MirrorsSQL.Get_work_id.replace("PARAM1", mirrorContext.mirrorDataFromStg.get(i).PARENT_PMX_SOURCE);
            Log.info(sql);
            mirrorContext.workID = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);
            fWorkID = mirrorContext.workID.get(0).workID;

            sql=MirrorsSQL.Get_child_id.replace("PARAM1", mirrorContext.mirrorDataFromStg.get(i).CHILD_PMX_SOURCE);
            mirrorContext.childID = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);

            sql = String.format(MirrorsSQL.GET_SA_mirror_DATA.replace("PARAM1",fWorkID)
                    .replace("PARAM2",mirrorContext.childID.get(0).workID));
            mirrorContext.mirrorDataFromSA = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + mirrorContext.workID.get(0).workID,
                    "WorkRelationship",
                    mirrorContext.mirrorDataFromSA.get(0).B_CLASSNAME);

            if (mirrorContext.mirrorDataFromStg.get(i).EFFECTIVE_START_DATE != null
                    || mirrorContext.mirrorDataFromSA.get(i).EFFECTIVE_START_DATE != null) {
                Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + mirrorContext.workID.get(0).workID,
                        mirrorContext.mirrorDataFromStg.get(i).EFFECTIVE_START_DATE,
                        mirrorContext.mirrorDataFromSA.get(0).EFFECTIVE_START_DATE);
            }
            /*sql=MirrorsSQL.Get_mirror_id.replace("PARAM1", "WORK_TRANS-"+
                    mirrorContext.mirrorDataFromStg.get(i).RELATIONSHIP_PMX_SOURCEREF);
            Log.info(sql);
            mirrorContext.mirrorID = DBManager.getDBResultAsBeanList(sql, MirrorsDataObject.class, Constants.EPH_URL);

            Assert.assertEquals("The WORK_REL_mirror_ID is incorrect for id=" + mirrorContext.workID.get(0).workID,
                    mirrorContext.mirrorID.get(0).mirrorId,
                    mirrorContext.mirrorDataFromSA.get(0).WORK_REL_mirror_ID);

            Assert.assertEquals("The F_RELATIONSHIP_TYPE is incorrect for id=" + mirrorContext.workID.get(0).workID,
                    mirrorContext.childID.get(0).workID,
                    mirrorContext.mirrorDataFromSA.get(0).CHILD_PMX_SOURCE);
*/
            if (mirrorContext.mirrorDataFromStg.get(i).ENDON != null
                    || mirrorContext.mirrorDataFromSA.get(0).ENDON != null) {
                Assert.assertEquals("The ENDON is incorrect for id=" + mirrorContext.workID,
                        mirrorContext.mirrorDataFromStg.get(i).ENDON,
                        mirrorContext.mirrorDataFromSA.get(0).ENDON);
            }
        }
    }

    @And("^The mirror data between SA and GD is identical$")
    public void checkmirrorGDData(){
        for (int i=0; i<mirrorContext.mirrorDataFromSAall.size();i++) {
            Assert.assertEquals("The WORK_REL_mirror_ID is incorrect for id=" + mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                    mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                    mirrorContext.mirrorDataFromGD.get(i).WORK_REL_mirror_ID);

            Assert.assertEquals("The RELATIONSHIP_PMX_SOURCEREF is incorrect for id=" + mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                    mirrorContext.mirrorDataFromSAall.get(i).RELATIONSHIP_PMX_SOURCEREF,
                    mirrorContext.mirrorDataFromGD.get(i).RELATIONSHIP_PMX_SOURCEREF);

            Assert.assertEquals("The CHILD_PMX_SOURCE is incorrect for id=" + mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                    mirrorContext.mirrorDataFromSAall.get(i).CHILD_PMX_SOURCE,
                    mirrorContext.mirrorDataFromGD.get(i).CHILD_PMX_SOURCE);

            Assert.assertEquals("The B_CLASSNAME is incorrect for id=" +mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                    mirrorContext.mirrorDataFromSAall.get(i).B_CLASSNAME,
                    mirrorContext.mirrorDataFromGD.get(i).B_CLASSNAME);

            if (mirrorContext.mirrorDataFromSAall.get(i).EFFECTIVE_START_DATE != null
                    || mirrorContext.mirrorDataFromGD.get(i).EFFECTIVE_START_DATE != null) {
                Assert.assertEquals("The EFFECTIVE_START_DATE is incorrect for id=" + mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                        mirrorContext.mirrorDataFromSAall.get(i).EFFECTIVE_START_DATE,
                        mirrorContext.mirrorDataFromGD.get(i).EFFECTIVE_START_DATE);
            }
            if (mirrorContext.mirrorDataFromSAall.get(i).ENDON != null
                    || mirrorContext.mirrorDataFromGD.get(i).ENDON != null) {
                Assert.assertEquals("The ENDON is incorrect for id=" + mirrorContext.mirrorDataFromSAall.get(i).WORK_REL_mirror_ID,
                        mirrorContext.mirrorDataFromSAall.get(i).ENDON,
                        mirrorContext.mirrorDataFromGD.get(i).ENDON);
            }
        }
    }
}


