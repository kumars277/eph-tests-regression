package com.eph.automation.testing.web.steps.datalake;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityDLContext;
import com.eph.automation.testing.models.dao.datalake.*;
import com.eph.automation.testing.services.db.DataLakeSql.WorksDataChecksSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class WorksDataCheckSteps {

    @StaticInjection
    public DataQualityDLContext dataQualityDLContext;
    private static String sql;
    private static List<String> workIds;


    @Given("^We get (.*) random ids of work")
    public void getRandomWorkIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        //Log.info("numberOfRecords = " + numberOfRecords);

        sql = String.format(WorksDataChecksSQL.GET_RANDOM_WORK_ID, numberOfRecords);
        Log.info(sql);
        List<Map<?, ?>> randomWorkIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        workIds = randomWorkIds.stream().map(m -> (String) m.get("WORK_ID")).collect(Collectors.toList());
        Log.info(workIds.toString());
    }

    @When("^We get the work records from EPH$")
    public void getWorksEPH() {
        Log.info("We get the work records from EPH..");
        sql = String.format(WorksDataChecksSQL.GET_DATA_WORKS_EPH, Joiner.on("','").join(workIds));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromEPH = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.EPH_URL);
    }


    @Then("^We get the work records from DL$")
    public void getWorksDL() {
        Log.info("We get the work records from DL..");
        sql = String.format(WorksDataChecksSQL.GET_DATA_WORKS_DL, Joiner.on("','").join(workIds));
        Log.info(sql);
        dataQualityDLContext.tbWorkDataObjectsFromDL = DBManager.getDBResultAsBeanList(sql, WorkDataDLObject.class, Constants.AWS_URL);
    }

    @And("^Compare work records in EPH and DL$")
    public void compareDataEPHtoDL(){

        Log.info("Sorting the data to compare the work records in EPH and DATA LAKE ..");//sort data in the lists
        for (int i=0; i<dataQualityDLContext.tbWorkDataObjectsFromEPH.size();i++) {
            dataQualityDLContext.tbWorkDataObjectsFromEPH.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));
            dataQualityDLContext.tbWorkDataObjectsFromDL.sort(Comparator.comparing(WorkDataDLObject::getWORK_ID));


            System.out.println("EPH Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE());
            System.out.println("DATALAKE Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());


            System.out.println("EPH Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR());
            System.out.println("DATALAKE Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());

            System.out.println("EPH Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC());
            System.out.println("DATALAKE Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());

            System.out.println("EPH Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT());
            System.out.println("DATALAKE Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());

            System.out.println("EPH Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE());
            System.out.println("DATALAKE Date for WORK ID =>"+workIds.get(i)+"="+dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());


            if(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME()!=null ||
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME()!=null){
                Assert.assertEquals("The B_CLASSNAME is incorrect for id=" + workIds.get(i),
                        dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CLASSNAME(),
                        dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CLASSNAME());
            }

            Assert.assertEquals("The B_BATCHID is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_BATCHID(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_BATCHID());


            if(dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE()!=null ||
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE()!=null){
                Assert.assertEquals("The B_CREDATE is incorrect for id=" + workIds.get(i),
                        dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREDATE(),
                        dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREDATE());
            }


            Assert.assertEquals("The B_UPDATE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDDATE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDDATE());

            Assert.assertEquals("The B_CREATOR is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_CREATOR(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_CREATOR());

            Assert.assertEquals("The B_UPDATOR is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getB_UPDATOR(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getB_UPDATOR());


            Assert.assertEquals("The S_WORK_ID is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_ID(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_ID());

            Assert.assertEquals("The EXTERNAL_REFERENCE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEXTERNAL_REFERENCE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEXTERNAL_REFERENCE());

            Assert.assertEquals("The WORK_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_TITLE());

            Assert.assertEquals("The S_WORK_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_TITLE());

            Assert.assertEquals("The WORK_SUB_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SUB_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SUB_TITLE());

            Assert.assertEquals("The S_WORK_SUB_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SUB_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SUB_TITLE());

            Assert.assertEquals("The WORK_SHORT_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getWORK_SHORT_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getWORK_SHORT_TITLE());

            Assert.assertEquals("The S_WORK_SHORT_TITLE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getS_WORK_SHORT_TITLE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getS_WORK_SHORT_TITLE());

            Assert.assertEquals("The ELECTRO_RIGHTS_INDICATOR is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getELECTRO_RIGHTS_INDICATOR(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getELECTRO_RIGHTS_INDICATOR());

            Assert.assertEquals("The VOLUME is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getVOLUME(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getVOLUME());

            Assert.assertEquals("The COPYRIGHT_YEAR is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getCOPYRIGHT_YEAR(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getCOPYRIGHT_YEAR());

            Assert.assertEquals("The EDITION_NUMBER is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getEDITION_NUMBER(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getEDITION_NUMBER());

            Assert.assertEquals("The T_SUMMARY_CHANGED is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_SUMMARY_CHANGED(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_SUMMARY_CHANGED());

            Assert.assertEquals("The T_EVENT_DESCRIPTION is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getT_EVENT_DESCRIPTION(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getT_EVENT_DESCRIPTION());

            Assert.assertEquals("The F_TYPE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_TYPE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_TYPE());

            Assert.assertEquals("The F_STATUS is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_STATUS(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_STATUS());

            Assert.assertEquals("The f_accountable_product is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_accountable_product(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_accountable_product());

            Assert.assertEquals("The F_PMC is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_PMC(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_PMC());

            Assert.assertEquals("The F_OA_TYPE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_OA_TYPE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_OA_TYPE());

            Assert.assertEquals("The F_FAMILY is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_FAMILY(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_FAMILY());

            Assert.assertEquals("The IMPRINT is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_IMPRINT(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_IMPRINT());

            Assert.assertEquals("The F_SOCIETY_OWNERSHIP is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SOCIETY_OWNERSHIP(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SOCIETY_OWNERSHIP());

            Assert.assertEquals("The SUBSCRIPTION_TYPE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SUBSCRIPTION_TYPE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SUBSCRIPTION_TYPE());

            Assert.assertEquals("The F_LLANGUAGE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_LLANGUAGE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_LLANGUAGE());

            Assert.assertEquals("The F_T_EVENT_TYPE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_T_EVENT_TYPE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_T_EVENT_TYPE());

            Assert.assertEquals("The F_EVENT is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_EVENT(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_EVENT());

            Assert.assertEquals("The F_SELF_ONE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_ONE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_ONE());

            Assert.assertEquals("The F_SELF_TWO is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_TWO(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_TWO());

            Assert.assertEquals("The F_SELF_THREE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_THREE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_THREE());

            Assert.assertEquals("The F_SELF_FOUR is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FOUR(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FOUR());

            Assert.assertEquals("The F_SELF_FIVE is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_FIVE(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_FIVE());

            Assert.assertEquals("The F_SELF_SIX is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SIX(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SIX());

            Assert.assertEquals("The F_SELF_SEVEN is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_SEVEN(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_SEVEN());

            Assert.assertEquals("The F_SELF_EIGHT is incorrect for id=" + workIds.get(i),
                    dataQualityDLContext.tbWorkDataObjectsFromEPH.get(i).getF_SELF_EIGHT(),
                    dataQualityDLContext.tbWorkDataObjectsFromDL.get(i).getF_SELF_EIGHT());


        }
    }





}
