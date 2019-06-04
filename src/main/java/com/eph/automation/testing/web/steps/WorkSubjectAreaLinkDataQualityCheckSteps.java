

package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.models.dao.WorkSubjectAreaLinkDataObject;
import com.eph.automation.testing.services.db.sql.WorkCountSQL;
import com.eph.automation.testing.services.db.sql.WorkSubjectAreaLinkDataSQL;
import com.google.common.base.Joiner;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Bistra Drazheva on 18/04/2019
 */
public class WorkSubjectAreaLinkDataQualityCheckSteps {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    private String sql;
    private static int countWorkSubjectAreaRecordsPMX;
    private static int countWorkSubjectAreaRecordsEPHSTG;
    private static int countWorkSubjectAreaRecordsEPHSTGDQ;
    private static int countWorkSubjectAreaRecordsEPHSA;
    private static int countWorkSubjectAreaRecordsEPHGD;
    private static List<String> ids;
    private static List<WorkDataObject> refreshDate;

    @Given("^We get the count of work subject area data from PMX$")
    public void getCountWorkSubjectAreaRecordsPMX() {
        Log.info("When We get the count of work subject area data in PMX ..");
        sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_PMX;
        Log.info(sql);
        List<Map<String, Object>> workSubjectAreaNumber = DBManager.getDBResultMap(sql, Constants.PMX_URL);
        countWorkSubjectAreaRecordsPMX = ((BigDecimal) workSubjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of work subject area data in PMX is: " + countWorkSubjectAreaRecordsPMX);
    }

    @When("^We get the count of work subject area data from EPH STG$")
    public void getCountWorkSubjectAreaRecordsEPHSTG() {
        Log.info("When We get the count of work subject area data in EPH STG ..");
        sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_STG;
        Log.info(sql);
        List<Map<String, Object>> workSubjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countWorkSubjectAreaRecordsEPHSTG = ((Long) workSubjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of work subject area data in EPH STG is: " + countWorkSubjectAreaRecordsEPHSTG);
    }


    @When("^We get the count of work subject area data from EPH STG With DQ$")
    public void getCountWorkSubjectAreaRecordsEPHSTGDQ() {
        Log.info("When We get the count of work subject area data in EPH STG with DQ ..");
        if (System.getProperty("LOAD") != null) {
            if(System.getProperty("LOAD").equalsIgnoreCase("FULL_LOAD")) {
                sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ;
                Log.info(sql);
                List<Map<String, Object>> workSubjectAreaNumberDQ = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                countWorkSubjectAreaRecordsEPHSTGDQ = ((Long) workSubjectAreaNumberDQ.get(0).get("count")).intValue();
                Log.info("Count of work subject area data in EPH STG with DQ is: " + countWorkSubjectAreaRecordsEPHSTGDQ);
            }else {
                sql = WorkCountSQL.GET_REFRESH_DATE;
                refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                        Constants.EPH_URL);

                sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ_Delta.replace("PARAM1"
                        ,refreshDate.get(1).refresh_timestamp);
                Log.info(sql);
                List<Map<String, Object>> workSubjectAreaNumberDQ = DBManager.getDBResultMap(sql, Constants.EPH_URL);
                countWorkSubjectAreaRecordsEPHSTGDQ = ((Long) workSubjectAreaNumberDQ.get(0).get("count")).intValue();
                Log.info("Count of work subject area data in EPH STG with DQ is: " + countWorkSubjectAreaRecordsEPHSTGDQ);
            }
        }else{
            sql = WorkCountSQL.GET_REFRESH_DATE;
            refreshDate =DBManager.getDBResultAsBeanList(sql, WorkDataObject.class,
                    Constants.EPH_URL);

            sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ_Delta.replace("PARAM1"
                    ,refreshDate.get(1).refresh_timestamp);
            Log.info(sql);
            List<Map<String, Object>> workSubjectAreaNumberDQ = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            countWorkSubjectAreaRecordsEPHSTGDQ = ((Long) workSubjectAreaNumberDQ.get(0).get("count")).intValue();
            Log.info("Count of work subject area data in EPH STG with DQ is: " + countWorkSubjectAreaRecordsEPHSTGDQ);/*
            sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_STG_DQ;
            Log.info(sql);
            List<Map<String, Object>> workSubjectAreaNumberDQ = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            countWorkSubjectAreaRecordsEPHSTGDQ = ((Long) workSubjectAreaNumberDQ.get(0).get("count")).intValue();
            Log.info("Count of work subject area data in EPH STG with DQ is: " + countWorkSubjectAreaRecordsEPHSTGDQ);*/
        }
    }

    @When("^We get the count of work subject area data from EPH SA$")
    public void getCountWorkSubjectAreaRecordsEPHSA() {
        Log.info("When We get the count of work subject area data in EPH SA ..");
        sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_SA;
        Log.info(sql);
        List<Map<String, Object>> workSubjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countWorkSubjectAreaRecordsEPHSA = ((Long) workSubjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of work subject area data in EPH SA is: " + countWorkSubjectAreaRecordsEPHSA);
    }

    @Then("^We get the count of work subject area data from EPH GD$")
    public void getCountWorkSubjectAreaRecordsEPHGD() {
        Log.info("When We get the count of work subject area data in EPH GD ..");
        sql = WorkSubjectAreaLinkDataSQL.SELECT_COUNT_WORK_SUBJECT_AREA_GD;
        Log.info(sql);
        List<Map<String, Object>> workSubjectAreaNumber = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        countWorkSubjectAreaRecordsEPHGD = ((Long) workSubjectAreaNumber.get(0).get("count")).intValue();
        Log.info("Count of work subject area data in EPH GD is: " + countWorkSubjectAreaRecordsEPHGD);
    }


    @Then("^Compare the count of work subject area data in PMX and EPH STG$")
    public void compareCountWorkSubjectAreaRecordsBetweenPMXAndEPHSTG() {
        assertEquals("\nWork Subject area data count in PMX and EPH STG is not equal", countWorkSubjectAreaRecordsPMX, countWorkSubjectAreaRecordsEPHSTG);

    }

    @Then("^Compare the count of work subject area data in EPH STG and EPH SA$")
    public void compareCountWorkSubjectAreaRecordsBetweenEPHSTGAndEPHSA() {
        assertEquals("\nWork Subject area data count in EPH STG and EPH SA is not equal", countWorkSubjectAreaRecordsEPHSTGDQ, countWorkSubjectAreaRecordsEPHSA);

    }

    @Then("^Compare the count of work subject area data in EPH SA and EPH GD$")
    public void compareCountWorkSubjectAreaRecordsBetweenEPHSAAndEPHGD() {
        assertEquals("\nWork Subject area data count in EPH SA and EPH GD is not equal", countWorkSubjectAreaRecordsEPHSA, countWorkSubjectAreaRecordsEPHGD);
    }


    @Given("^We get (.*) random ids of work subject area data$")
    public void getWorkRandomSubjectAreaDataIds(String numberOfRecords) {
        Log.info("Get random records ..");

        //Get property when run with jenkins
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        Log.info("numberOfRecords = " + numberOfRecords);

        Log.info("Get the ids from stg ...");
        sql = String.format(WorkSubjectAreaLinkDataSQL.SELECT_RANDOM_IDS, numberOfRecords);
        Log.info(sql);

        List<Map<?, ?>> randomIds = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        ids = randomIds.stream().map(m -> (BigDecimal) m.get("PRODUCT_SUBJECT_AREA_ID")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }

    @When("^We get the work subject area data from PMX$")
    public void getWorkSubjectAreaDataPMX() {
        Log.info("Get the work subject area data from PMX  ..");
        sql = String.format(WorkSubjectAreaLinkDataSQL.EXTRACT_DATA_WORK_SUBJECT_AREA_PMX, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workSubjectAreaDataObjectsFromPMX = DBManager
                .getDBResultAsBeanList(sql, WorkSubjectAreaLinkDataObject.class, Constants.PMX_URL);
    }

    @When("^We get the work subject area data from EPH STG$")
    public void getWorkSubjectAreaDataEPHSTG() {
        Log.info("Get the work subject area data from EPH STG  ..");
        sql = String.format(WorkSubjectAreaLinkDataSQL.EXTRACT_DATA_WORK_SUBJECT_AREA_STG, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workSubjectAreaDataObjectsFromSTG = DBManager
                .getDBResultAsBeanList(sql, WorkSubjectAreaLinkDataObject.class, Constants.EPH_URL);
    }

    @When("^We get the work subject area data from EPH SA$")
    public void getSubjectAreaDataEPHSA() {
        Log.info("Get the work subject area data from EPH SA  ..");
        sql = String.format(WorkSubjectAreaLinkDataSQL.EXTRACT_DATA_WORK_SUBJECT_AREA_SA, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workSubjectAreaDataObjectsFromSA = DBManager
                .getDBResultAsBeanList(sql, WorkSubjectAreaLinkDataObject.class, Constants.EPH_URL);
    }

    @And("^Check the mandatory columns are populated for work subject link$")
    public void checkMandatoryColumnsForWorkSubjectAreaLinkInSAArePopulated() {
        Log.info("We check that mandatory columns are populated ...");

        IntStream.range(0, dataQualityContext.workSubjectAreaDataObjectsFromSA.size()).forEach(i -> {

            //verify WORK_SUBJECT_AREA_LINK_ID is not null
            assertNotNull(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getWORK_SUBJECT_AREA_LINK_ID());
            //verify f_subject_area is not null
            assertNotNull(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_SUBJECT_AREA());
            //verify f_wwork
            assertNotNull(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_WWORK());


        });
    }

    @When("^We get the work subject area data from EPH GD$")
    public void getWorkSubjectAreaDataEPHGD() {
        Log.info("Get the work subject area data from EPH GD  ..");
        sql = String.format(WorkSubjectAreaLinkDataSQL.EXTRACT_DATA_WORK_SUBJECT_AREA_GD, Joiner.on("','").join(ids));
        Log.info(sql);

        dataQualityContext.workSubjectAreaDataObjectsFromGD = DBManager
                .getDBResultAsBeanList(sql, WorkSubjectAreaLinkDataObject.class, Constants.EPH_URL);
    }

    @And("^Compare the work subject area data in PMX and EPH STG$")
    public void compareWorkSubjectAreaDataPMXAndSTG() {
        Log.info("And compare the work subject area data in PMX and EPH STG ..");
        dataQualityContext.workSubjectAreaDataObjectsFromPMX.sort(Comparator.comparing(WorkSubjectAreaLinkDataObject::getPRODUCT_SUBJECT_AREA_ID));
        dataQualityContext.workSubjectAreaDataObjectsFromSTG.sort(Comparator.comparing(WorkSubjectAreaLinkDataObject::getPRODUCT_SUBJECT_AREA_ID));


        IntStream.range(0, dataQualityContext.workSubjectAreaDataObjectsFromSTG.size()).forEach(i -> {

            //PRODUCT_SUBJECT_AREA_ID
            Log.info("PRODUCT_SUBJECT_AREA_ID in PMX: " + dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getPRODUCT_SUBJECT_AREA_ID());
            Log.info("PRODUCT_SUBJECT_AREA_ID in EPH STG: " + dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getPRODUCT_SUBJECT_AREA_ID());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getPRODUCT_SUBJECT_AREA_ID(), dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getPRODUCT_SUBJECT_AREA_ID());

            //F_SUBJECT_AREA
            Log.info("F_SUBJECT_AREA in PMX: " + dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getF_SUBJECT_AREA());
            Log.info("F_SUBJECT_AREA in EPH STG: " + dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getF_SUBJECT_AREA());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getF_SUBJECT_AREA(), dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getF_SUBJECT_AREA());

            //F_PRODUCT_WORK
            Log.info("F_PRODUCT_WORK in PMX: " + dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getF_PRODUCT_WORK());
            Log.info("F_PRODUCT_WORK in EPH STG: " + dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getF_PRODUCT_WORK());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getF_PRODUCT_WORK(), dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getF_PRODUCT_WORK());

            Log.info("START_DATE in PMX: " + dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getSTART_DATE());
            Log.info("START_DATE in EPH STG: " + dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getSTART_DATE());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getSTART_DATE().substring(0,10)
                    , dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getSTART_DATE());

            if (dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getEFFTO_DATE()!=null
                    || dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getEFFTO_DATE() != null) {
                Log.info("END_DATE in PMX: " + dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getEFFTO_DATE());
                Log.info("END_DATE in EPH STG: " + dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getEFFTO_DATE());
            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getEFFTO_DATE().substring(0,10)
                    , dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getEFFTO_DATE());
            }

            Date pmxUpdatedDate = null;
            try {
                pmxUpdatedDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS").parse(dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getUPDATED());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date stgDate = null;
            try {
                stgDate = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSSSSS aaa").parse(dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getUPDATED());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            if (dataQualityContext.workSubjectAreaDataObjectsFromPMX.get(i).getUPDATED()!=null
                    || dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(i).getUPDATED() != null) {
                assertTrue("Expecting the UPDATED details from PMX and EPH Consistent for id=" + ids.get(i),
                        pmxUpdatedDate
                                .equals(stgDate));
            }

        });
    }

    @And("^Compare the work subject area data in EPH STG and EPH SA$")
    public void compareWorkSubjectAreaDataSTGANDSA() {
        Log.info("And compare the work subject area data in EPH STG and EPH SA..");


        Log.info("The Size of SA list is : " +  dataQualityContext.workSubjectAreaDataObjectsFromSA.size() );

        IntStream.range(0, dataQualityContext.workSubjectAreaDataObjectsFromSA.size()).forEach(i -> {
            sql = String.format(WorkSubjectAreaLinkDataSQL.SELECT_DATA_FROM_STG_FOR_CURRENT_RECORD_FROM_SA, dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getWORK_SUBJECT_AREA_LINK_ID());
            Log.info(sql);

            dataQualityContext.workSubjectAreaDataObjectsFromSTG = DBManager
                    .getDBResultAsBeanList(sql, WorkSubjectAreaLinkDataObject.class, Constants.EPH_URL);

            //B_CLASSNAME
            Log.info("B_CLASSNAME in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());

            assertEquals("WorkSubjectAreaLink", dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());


            sql = String.format(WorkSubjectAreaLinkDataSQL.GET_F_SUBJECT_AREA, dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(0).getF_SUBJECT_AREA());
            Log.info(sql);
            List<Map<String, Object>> subjectAreaObject = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            int F_SUBJECT_AREA = ((BigDecimal) subjectAreaObject.get(0).get("F_SUBJECT_AREA")).intValue();

            //F_SUBJECT_AREA
            Log.info("F_SUBJECT_AREA in EPH STG: " + F_SUBJECT_AREA);
            Log.info("F_SUBJECT_AREA in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_SUBJECT_AREA());

            assertEquals(String.valueOf(F_SUBJECT_AREA), dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_SUBJECT_AREA());

            sql = String.format(WorkSubjectAreaLinkDataSQL.GET_F_WWORK, dataQualityContext.workSubjectAreaDataObjectsFromSTG.get(0).getF_PRODUCT_WORK());
            Log.info(sql);
            List<Map<String, Object>> fWworkObject = DBManager.getDBResultMap(sql, Constants.EPH_URL);
            String F_WWORK = fWworkObject.get(0).get("F_WWORK").toString();


            //F_WWORK
            Log.info("F_WWORK in EPH STG: " + F_WWORK);
            Log.info("F_WWORK in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_WWORK());

            assertEquals(F_WWORK, dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_WWORK());
        });

    }


    @And("^Compare the work subject area data in EPH SA and EPH GD$")
    public void compareWorkSubjectAreaDataSAANDGD() {
        Log.info("And compare the work subject area data in SA and EPH GD ..");
        dataQualityContext.workSubjectAreaDataObjectsFromSA.sort(Comparator.comparing(WorkSubjectAreaLinkDataObject::getWORK_SUBJECT_AREA_LINK_ID));
        dataQualityContext.workSubjectAreaDataObjectsFromGD.sort(Comparator.comparing(WorkSubjectAreaLinkDataObject::getWORK_SUBJECT_AREA_LINK_ID));


        IntStream.range(0, dataQualityContext.workSubjectAreaDataObjectsFromSA.size()).forEach(i -> {

            //B_CLASSNAME
            Log.info("B_CLASSNAME in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME());
            Log.info("B_CLASSNAME in GD: " + dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getB_CLASSNAME());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getB_CLASSNAME(), dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getB_CLASSNAME());

            //F_SUBJECT_AREA
            Log.info("F_SUBJECT_AREA in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_SUBJECT_AREA());
            Log.info("F_SUBJECT_AREA in GD: " + dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getF_SUBJECT_AREA());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_SUBJECT_AREA(), dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getF_SUBJECT_AREA());


            //F_WWORK
            Log.info("F_WWORK in SA: " + dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_WWORK());
            Log.info("F_WWORK in GD: " + dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getF_WWORK());

            assertEquals(dataQualityContext.workSubjectAreaDataObjectsFromSA.get(i).getF_WWORK(), dataQualityContext.workSubjectAreaDataObjectsFromGD.get(i).getF_WWORK());

        });

    }
}
