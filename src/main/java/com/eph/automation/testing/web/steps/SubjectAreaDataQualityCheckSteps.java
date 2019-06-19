package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.SubjectAreaDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.SubjectAreaDataSQL;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Bistra Drazheva on 17/04/2019
 */
public class SubjectAreaDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countSubjectAreaRecordsPMX;
    private static int countSubjectAreaRecordsEPHSTG;
    private static int countSubjectAreaRecordsEPHSA;
    private static int countSubjectAreaRecordsEPHGD;
    private static List<String> ids;
    private static List<WorkDataObject> refreshDate;

    @Given("^We get the count of subject area data from PMX$")
    public void getCountSubjectAreaRecordsPMX() {
        Log.info("When We get the count of subject area data in PMX ..");
        sql = SubjectAreaDataSQL.SELECT_COUNT_SUBJECT_AREA_PMX;
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countSubjectAreaRecordsPMX = ((BigDecimal) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of subject area data in PMX is: " + countSubjectAreaRecordsPMX);
    }

    @When("^We get the count of subject area data from EPH STG$")
    public void getCountSubjectAreaRecordsEPHSTG() {
        Log.info("When We get the count of subject area data in EPH STG ..");

        if (System.getProperty("LOAD") == null || System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
            sql = SubjectAreaDataSQL.SELECT_COUNT_SUBJECT_AREA_STG;
            Log.info(sql);
            List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            countSubjectAreaRecordsEPHSTG = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
            Log.info("Count of subject area data in EPH STG is: " + countSubjectAreaRecordsEPHSTG);
        } else {
        sql = WorkCountSQL.GET_REFRESH_DATE;
        refreshDate = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                Constants.EPH_URL);
        Log.info("refresh date : " + refreshDate);


        sql = SubjectAreaDataSQL.SELECT_COUNT_SUBJECT_AREA_STG_Delta.replace("PARAM1", refreshDate.get(1).refresh_timestamp);
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countSubjectAreaRecordsEPHSTG = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of subject area data in EPH STG is: " + countSubjectAreaRecordsEPHSTG);
        }
    }

    @When("^We get the count of subject area data from EPH SA$")
    public void getCountSubjectAreaRecordsEPHSA() {
        Log.info("When We get the count of subject area data in EPH SA ..");
        sql = SubjectAreaDataSQL.SELECT_COUNT_SUBJECT_AREA_SA;
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countSubjectAreaRecordsEPHSA = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of subject area data in EPH SA is: " + countSubjectAreaRecordsEPHSA);
    }

    @Then("^We get the count of subject area data from EPH GD$")
    public void getCountSubjectAreaRecordsEPHGD() {
        Log.info("When We get the count of subject area data in EPH GD ..");
        sql = SubjectAreaDataSQL.SELECT_COUNT_SUBJECT_AREA_GD;
        Log.info(sql);
        List<Map<String, Object>> subjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countSubjectAreaRecordsEPHGD = ((Long) subjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of subject area data in EPH GD is: " + countSubjectAreaRecordsEPHGD);
    }


    @Then("^Compare the count of subject area data in PMX and EPH STG$")
    public void compareCountSubjectAreaRecordsBetweenPMXAndEPHSTG() {
        assertEquals("\nSubject area data count in PMX and EPH STG is not equal", countSubjectAreaRecordsPMX, countSubjectAreaRecordsEPHSTG);

    }

    @Then("^Compare the count of subject area data in EPH STG and EPH SA$")
    public void compareCountSubjectAreaRecordsBetweenEPHSTGAndEPHSA() {
        assertEquals("\nSubject area data count in EPH STG and EPH SA is not equal", countSubjectAreaRecordsEPHSTG, countSubjectAreaRecordsEPHSA);

    }

    @Then("^Compare the count of subject area data in EPH SA and EPH GD$")
    public void compareCountSubjectAreaRecordsBetweenEPHSAAndEPHGD() {
        assertEquals("\nSubject area data count in EPH SA and EPH GD is not equal", countSubjectAreaRecordsEPHSA, countSubjectAreaRecordsEPHGD);
    }


    @Given("^We get (.*) random ids of subject area data$")
    public void getRandomSubjectAreaDataIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber") == null) {
            numberOfRecords = "5";
        } else {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        Log.info("Get the ids from stg ...");
        sql = String.format(SubjectAreaDataSQL.SELECT_RANDOM_SUBJECT_DATA, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomIds.stream().map(m -> (BigDecimal) m.get("PMX_SOURCE_REF")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }

    @When("^We get the subject area data from PMX$")
    public void getSubjectAreaDataPMX() {
        Log.info("Get the subject area data from PMX  ..");
        sql = String.format(SubjectAreaDataSQL.EXTRACT_DATA_SUBJECT_AREA_PMX, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.subjectAreaDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.PMX_URL);
    }

    @When("^We get the subject area data from EPH STG$")
    public void getSubjectAreaDataEPHSTG() {
        Log.info("Get the subject area data from EPH STG  ..");
        sql = String.format(SubjectAreaDataSQL.EXTRACT_DATA_SUBJECT_AREA_STG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.subjectAreaDataObjectsFromSTG = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.EPH_URL);
    }

    @When("^We get the subject area data from EPH SA$")
    public void getSubjectAreaDataEPHSA() {
        Log.info("Get the subject area data from EPH SA  ..");
        sql = String.format(SubjectAreaDataSQL.EXTRACT_DATA_SUBJECT_AREA_SA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.subjectAreaDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.EPH_URL);
    }

    @When("^We get the subject area data from EPH GD$")
    public void getSubjectAreaDataEPHGD() {
        Log.info("Get the subject area data from EPH GD  ..");
        sql = String.format(SubjectAreaDataSQL.EXTRACT_DATA_SUBJECT_AREA_GD, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.subjectAreaDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, SubjectAreaDataObject.class, Constants.EPH_URL);
    }

    @And("^Check the mandatory columns are populated for subject link$")
    public void checkMandatoryColumnsForSubjectAreaInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

        IntStream.range(0, dataQualityContext.subjectAreaDataObjectsFromSA.size()).forEach(i -> {

            //verify SUBJECT_AREA_ID is not null
            assertNotNull(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_ID());
            //verify SUBJECT_AREA_CODE is not null
            assertNotNull(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_CODE());
            //verify SUBJECT_AREA_NAME
            assertNotNull(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE());
            //verify SUBJECT_AREA_TYPE
            assertNotNull(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE());

        });
    }

    @And("^Compare the subject area data in PMX and EPH STG$")
    public void compareSubjectAreaDataPMXAndSTG() {
        Log.info("And compare the subject area data in PMX and EPH STG ..");
        dataQualityContext.subjectAreaDataObjectsFromPMX.sort(Comparator.comparing(SubjectAreaDataObject::getPMX_SOURCE_REF));
        dataQualityContext.subjectAreaDataObjectsFromSTG.sort(Comparator.comparing(SubjectAreaDataObject::getPMX_SOURCE_REF));


        IntStream.range(0, dataQualityContext.subjectAreaDataObjectsFromSTG.size()).forEach(i -> {

            //PMX_SOURCE_REF
            Log.info("PMX_SOURCE_REF in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getPMX_SOURCE_REF());
            Log.info("PMX_SOURCE_REF in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPMX_SOURCE_REF());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getPMX_SOURCE_REF(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPMX_SOURCE_REF());

            //SUBJECT_AREA_CODE
            Log.info("SUBJECT_AREA_CODE in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_CODE());
            Log.info("SUBJECT_AREA_CODE in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_CODE());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_CODE(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_CODE());

            //SUBJECT_AREA_NAME
            Log.info("SUBJECT_AREA_NAME in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_NAME());
            Log.info("SUBJECT_AREA_NAME in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_NAME());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_NAME(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_NAME());

            //PARENT_SUBJECT_AREA_REF
            Log.info("PARENT_SUBJECT_AREA_REF in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getPARENT_SUBJECT_AREA_REF());
            Log.info("PARENT_SUBJECT_AREA_REF in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPARENT_SUBJECT_AREA_REF());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getPARENT_SUBJECT_AREA_REF(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPARENT_SUBJECT_AREA_REF());


            //SUBJECT_AREA_TYPE
            Log.info("SUBJECT_AREA_TYPE in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_TYPE());
            Log.info("SUBJECT_AREA_TYPE in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_TYPE());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getSUBJECT_AREA_TYPE(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_TYPE());

            //UPDATED
            Log.info("UPDATED in PMX: " + dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getUPDATED());
            Log.info("UPDATED in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getUPDATED());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromPMX.get(i).getUPDATED(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getUPDATED());

        });

    }


    @And("^Compare the subject area data in EPH STG and EPH SA$")
    public void compareSubjectAreaDataEPHSTGAndEPHSA() {
        Log.info("And compare the subject area data in EPH STG and EPH SA..");
        dataQualityContext.subjectAreaDataObjectsFromSTG.sort(Comparator.comparing(SubjectAreaDataObject::getSUBJECT_AREA_CODE));
        dataQualityContext.subjectAreaDataObjectsFromSA.sort(Comparator.comparing(SubjectAreaDataObject::getSUBJECT_AREA_CODE));

        IntStream.range(0, dataQualityContext.subjectAreaDataObjectsFromSTG.size()).forEach(i -> {

            //B_CLASSNAME
            Log.info("B_CLASSNAME in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());

            assertEquals("SubjectArea", dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());

            //SUBJECT_AREA_CODE
            Log.info("SUBJECT_AREA_CODE in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_CODE());
            Log.info("SUBJECT_AREA_CODE in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_CODE());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_CODE(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_CODE());

            //SUBJECT_AREA_NAME
            Log.info("SUBJECT_AREA_NAME in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_NAME());
            Log.info("SUBJECT_AREA_NAME in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_NAME());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_NAME(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_NAME());


            //SUBJECT_AREA_TYPE
            Log.info("SUBJECT_AREA_TYPE in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_TYPE());
            Log.info("SUBJECT_AREA_TYPE in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE());


            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE(), dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getSUBJECT_AREA_TYPE());

            //F_PARENT_SUBJECT_AREA
            Log.info("PARENT_SUBJECT_AREA_REF in EPH STG: " + dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPARENT_SUBJECT_AREA_REF());
            Log.info("F_PARENT_SUBJECT_AREA in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getF_PARENT_SUBJECT_AREA());

            if (dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPARENT_SUBJECT_AREA_REF() == null)
                assertNull(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getF_PARENT_SUBJECT_AREA());
            else {
                sql = String.format(SubjectAreaDataSQL.GET_F_PARENT_SUBJECT_AREA, dataQualityContext.subjectAreaDataObjectsFromSTG.get(i).getPARENT_SUBJECT_AREA_REF());
                Log.info(sql);
                List<Map<String, Object>> subjectAreaObject = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                int F_PARENT_SUBJECT_AREA_SA = ((BigDecimal) subjectAreaObject.get(0).get("F_PARENT_SUBJECT_AREA")).intValue();
                assertEquals(F_PARENT_SUBJECT_AREA_SA, dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getF_PARENT_SUBJECT_AREA());
            }
        });

    }

    @And("^Compare the subject area data in EPH SA and EPH GD$")
    public void compareSubjectAreaDataEPHSAndEPHGD() {
        Log.info("And compare the subject area data in EPH SA and EPH GD..");
        dataQualityContext.subjectAreaDataObjectsFromSA.sort(Comparator.comparing(SubjectAreaDataObject::getSUBJECT_AREA_ID));
        dataQualityContext.subjectAreaDataObjectsFromGD.sort(Comparator.comparing(SubjectAreaDataObject::getSUBJECT_AREA_ID));

        IntStream.range(0, dataQualityContext.subjectAreaDataObjectsFromSA.size()).forEach(i -> {

            //B_CLASSNAME
            Log.info("B_CLASSNAME in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in GD: " + dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getB_CLASSNAME());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME(), dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getB_CLASSNAME());

            //SUBJECT_AREA_CODE
            Log.info("SUBJECT_AREA_CODE in EPH GD: " + dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_CODE());
            Log.info("SUBJECT_AREA_CODE in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_CODE());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_CODE(), dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_CODE());

            //SUBJECT_AREA_NAME
            Log.info("SUBJECT_AREA_NAME in EPH GD: " + dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_NAME());
            Log.info("SUBJECT_AREA_NAME in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_NAME());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_NAME(), dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_NAME());


            //SUBJECT_AREA_TYPE
            Log.info("SUBJECT_AREA_TYPE in EPH GD: " + dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_TYPE());
            Log.info("SUBJECT_AREA_TYPE in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE());


            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getSUBJECT_AREA_TYPE(), dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getSUBJECT_AREA_TYPE());

            //F_PARENT_SUBJECT_AREA
            Log.info("PARENT_SUBJECT_AREA_REF in EPH GD: " + dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getF_PARENT_SUBJECT_AREA());
            Log.info("F_PARENT_SUBJECT_AREA in SA: " + dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getF_PARENT_SUBJECT_AREA());

            assertEquals(dataQualityContext.subjectAreaDataObjectsFromSA.get(i).getF_PARENT_SUBJECT_AREA(), dataQualityContext.subjectAreaDataObjectsFromGD.get(i).getF_PARENT_SUBJECT_AREA());

        });

    }
}
