package com.eph.automation.testing.web.steps;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.dao.ManifestationDataObject;
import com.eph.automation.testing.models.dao.WorkDataObject;
import com.eph.automation.testing.services.db.sql.GeneratingRandomSQL;
import com.eph.automation.testing.services.db.sql.WorkExtractSQL;
import com.eph.automation.testing.services.db.sql.WorkDataCheckSQL;
import com.google.common.base.Joiner;
import com.jcraft.jsch.Logger;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;
import com.eph.automation.testing.helper.Log;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorksDataCheck {
    @StaticInjection
    public DataQualityContext dataQualityContext;
    public String sql;
    private static List<WorkDataObject> isbn;
    private List<WorkDataObject> workDataObjectsAccProd;
    private String numberOfRecords;
    private List<Map<?, ?>> manifestationIds;
    private static List<String> ids;
    private static List<String> isbns;

    @Given("^We have a number of works to check$")
    public void setWorkID(){
        Log.info("Get random records ..");

        //Get property when run with jenkins
        if (System.getProperty("dbRandomRecordsNumber")!=null) {
            numberOfRecords = System.getProperty("dbRandomRecordsNumber");
        }else {
            numberOfRecords = "10";
        }
        Log.info("numberOfRecords = " + numberOfRecords);

        sql = GeneratingRandomSQL.generatingValue
                    .replace("PARAM1", numberOfRecords);
        Log.info(sql);
        isbn = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);
/*
        id.get(i = isbn.get(i).random_value;*/

        List<Map<?, ?>> randomISBNIds = DBManager.getDBResultMapWithSetSchema(sql, Constants.EPH_URL);

        ids = randomISBNIds.stream().map(m -> (BigDecimal) m.get("random_value")).map(String::valueOf).collect(Collectors.toList());
        Log.info(ids.toString());
    }

    @When("^We get the product data from PMX, EPH Staging and EPH$")
    public void getPMXWorkData(){
        sql = String.format(WorkExtractSQL.PMX_WORK_EXTRACT, Joiner.on("','").join(ids));
        Log.info(sql);
        Log.info
         (sql);
            dataQualityContext.workDataObjectsFromSource = DBManager
                    .getDBResultAsBeanList(sql, WorkDataObject.class,Constants.PMX_URL);

        sql = String.format(WorkDataCheckSQL.GET_PMX_WORKS_STG_DATA, Joiner.on("','").join(ids));
        Log.info
         (sql);
        dataQualityContext.workDataObjectsFromPMXSTG = DBManager.getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);


        sql =  String.format(WorkDataCheckSQL.GET_STG_DQ_WORKS_DATA, Joiner.on("','").join(ids));
        Log.info
                (sql);
        dataQualityContext.workDataObjectsFromSTGDQ = DBManager
                .getDBResultMapWithSetSchema(sql, WorkDataObject.class, Constants.EPH_URL);

        sql =  String.format(WorkDataCheckSQL.GET_EPH_WORKS_DATA, Joiner.on("','").join(ids));
        Log.info
                (sql);
        dataQualityContext.workDataObjectsFromEPH = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);

        sql =  String.format(WorkDataCheckSQL.GET_EPH_GD_WORKS_DATA, Joiner.on("','").join(ids));
        Log.info
                (sql);
        dataQualityContext.workDataObjectsFromEPHGD = DBManager
                .getDBResultAsBeanList(sql, WorkDataObject.class, Constants.EPH_URL);


    }

    @Then("^The work data between PMX and EPH STG is identical$")
    public void checkPMXtoPMGSTGData() throws ParseException {
        if (dataQualityContext.workDataObjectsFromPMXSTG.isEmpty()&& System.getProperty("LOAD").equalsIgnoreCase("DELTA_LOAD")) {
            Log.info("There is no updated data for Works");
        } else {
        for (int i=0; i<dataQualityContext.workDataObjectsFromSource.size();i++) {
            Log.info(dataQualityContext.workDataObjectsFromSource.get(i).getWORK_TITLE());
            assertTrue("Expecting the Work title details from PMX and PMX Staging Consistent for id=" + ids.get(i),
                    dataQualityContext.workDataObjectsFromSource.get(i).getWORK_TITLE()
                            .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TITLE()));
            if (dataQualityContext.workDataObjectsFromSource.get(i).getWORK_SUBTITLE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE() != null) {
                assertTrue("Expecting the WORK_SUBTITLE details from PMX and EPH Consistent  for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getWORK_SUBTITLE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getDAC_KEY() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getDAC_KEY() != null) {
                assertTrue("Expecting the DAC_KEY details from PMX and EPH Consistent  for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getDAC_KEY()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getDAC_KEY()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getPRIMARY_ISBN() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRIMARY_ISBN() != null) {
                assertTrue("Expecting the PRIMARY_ISBN details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPRIMARY_ISBN()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRIMARY_ISBN()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getPROJECT_NUM() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPROJECT_NUM() != null) {
                assertTrue("Expecting the PROJECT_NUM from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPROJECT_NUM()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPROJECT_NUM()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getISSN_L() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getISSN_L() != null) {
                assertTrue("Expecting ISSN_L from PMX and EPH Consistent for id=",
                        dataQualityContext.workDataObjectsFromSource.get(i).getISSN_L()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getISSN_L()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getJOURNAL_NUMBER() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getJOURNAL_NUMBER() != null) {
                assertTrue("Expecting the JOURNAL_NUMBER details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getJOURNAL_NUMBER()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getJOURNAL_NUMBER()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getELECTRONIC_RIGHTS_IND() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getELECTRONIC_RIGHTS_IND() != null) {
                assertTrue("Expecting the ELECTRONIC_RIGHTS_IND details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getELECTRONIC_RIGHTS_IND()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getELECTRONIC_RIGHTS_IND()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getBOOK_EDITION_NAME() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_EDITION_NAME() != null) {
                assertTrue("Expecting the BOOK EDITION NAME details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getBOOK_EDITION_NAME()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_EDITION_NAME()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getBOOK_VOLUME_NAME() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_VOLUME_NAME() != null) {
                assertTrue("Expecting the BOOK VOLUME NAME details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getBOOK_VOLUME_NAME()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_VOLUME_NAME()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getPMC() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMC() != null) {
                assertTrue("Expecting the PMC details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPMC()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMC()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getPMG() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMG() != null) {
                assertTrue("Expecting the PMG details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPMG()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMG()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getWORK_ID() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_ID() != null) {
                assertTrue("Expecting the WORK_ID details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getWORK_ID()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_ID()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getWORK_STATUS() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_STATUS() != null) {
                assertTrue("Expecting the WORK_STATUS details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getWORK_STATUS()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_STATUS()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getWORK_SUBSTATUS() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBSTATUS() != null) {
                assertTrue("Expecting the WORK_SUBSTATUS details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getWORK_SUBSTATUS()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBSTATUS()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getWORK_TYPE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE() != null) {
                assertTrue("Expecting the WORK_TYPE details from PMX and EPH Consistent for id=",
                        dataQualityContext.workDataObjectsFromSource.get(i).getWORK_TYPE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getIMPRINT() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getIMPRINT() != null) {
                assertTrue("Expecting the IMPRINT details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getIMPRINT()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getIMPRINT()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null) {
                assertTrue("Expecting the OPEN_ACCESS_JNL_TYPE details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getPRODUCT_WORK_ID() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_ID() != null) {
                assertTrue("Expecting the PRODUCT_WORK_ID details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPRODUCT_WORK_ID()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_ID()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getF_ACC_PROD_HIERARCHY() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_ACC_PROD_HIERARCHY() != null) {
                assertTrue("Expecting the F_ACC_PROD_HIERARCHY details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getF_ACC_PROD_HIERARCHY()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_ACC_PROD_HIERARCHY()));
            }
            if (dataQualityContext.workDataObjectsFromSource.get(i).getF_RESPONSIBILITY_CENTRE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_RESPONSIBILITY_CENTRE() != null) {
                assertTrue("Expecting the F_RESPONSIBILITY_CENTRE details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getF_RESPONSIBILITY_CENTRE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_RESPONSIBILITY_CENTRE()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getF_OPCO_R12() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_OPCO_R12() != null) {
                assertTrue("Expecting the F_OPCO_R12 details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getF_OPCO_R12()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_OPCO_R12()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getPRODUCT_WORK_PUB_DATE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_PUB_DATE() != null) {
                assertTrue("Expecting the Product_WORK_PUB_DATE details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getPRODUCT_WORK_PUB_DATE().substring(0, 10)
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_PUB_DATE()));


            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getJOURNAL_ACRONYM() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getJOURNAL_ACRONYM() != null) {
                assertTrue("Expecting the Journal Acronym details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getJOURNAL_ACRONYM()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getJOURNAL_ACRONYM()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getOWNERSHIP() != null
                    && dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP() != null) {
                assertTrue("Expecting the Journal Acronym details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getOWNERSHIP()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getLANGUAGE_CODE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getLANGUAGE_CODE() != null) {
                assertTrue("Expecting the Language code details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getLANGUAGE_CODE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getLANGUAGE_CODE()));
            }

/*            Date pmxUpdatedDate = new SimpleDateFormat("dd-MMM-yy HH.mm.ss.SSSSSS").parse(dataQualityContext.workDataObjectsFromSource.get(i).UPDATED);
            Date stgDate = new SimpleDateFormat("YYYYMMDDHH24MM").parse(dataQualityContext.workDataObjectsFromPMXSTG.get(i).UPDATED);*/

            if (dataQualityContext.workDataObjectsFromSource.get(i).getUPDATED() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getUPDATED() != null) {
                assertTrue("Expecting the UPDATED details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getUPDATED()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getUPDATED()));
            }

            if (dataQualityContext.workDataObjectsFromSource.get(i).getRECORD_END_DATE() != null
                    || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getRECORD_END_DATE() != null) {
                assertTrue("Expecting the UPDATED details from PMX and EPH Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSource.get(i).getRECORD_END_DATE()
                                .equals(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getRECORD_END_DATE()));
            }
        }
        }
    }
    
    @And("^The work data between EPH STG and EPH DQ is identical$")
    public void checkPMXSTGandDQData() {
        for (int i=0; i<dataQualityContext.workDataObjectsFromSource.size();i++) {

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TITLE() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TITLE() != null) {
                if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TITLE().length() > 200) {
                    assertTrue("Expecting the Work title details from PMX Staging and STG DQ Consistent for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TITLE().substring(0, 200)
                                    .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TITLE()));
                } else {
                    assertTrue("Expecting the Work title details from PMX Staging and STG DQ Consistent for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TITLE()
                                    .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TITLE()));
                }
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_SUBTITLE() != null) {
                if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE().length()>200) {
                    assertTrue("Expecting the Work title details from PMX Staging and STG DQ Consistent for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE().substring(0, 200)
                                    .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_SUBTITLE()));
                }else {
                    assertTrue("Expecting the Work title details from PMX Staging and STG DQ Consistent for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBTITLE()
                                    .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_SUBTITLE()));
                }
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_PUB_DATE() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPRODUCT_WORK_PUB_DATE() != null) {

                System.out.print(dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_PUB_DATE().substring(0, 4) + " \n" +
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPRODUCT_WORK_PUB_DATE().substring(0, 4));
                assertTrue("Expecting the WORK_PUB_DATE details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPRODUCT_WORK_PUB_DATE().substring(0, 4)
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPRODUCT_WORK_PUB_DATE().substring(0, 4)));

            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getELECTRONIC_RIGHTS_IND() != null) {
                if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getELECTRONIC_RIGHTS_IND().equalsIgnoreCase("Y")) {
                    Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i)
                            , dataQualityContext.workDataObjectsFromSTGDQ.get(i).getELECTRONIC_RIGHTS_IND(), "1");
                } else {
                    Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getELECTRONIC_RIGHTS_IND(), "0");
                }
            }
            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() == null) {
                Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE(), "N");
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE().equalsIgnoreCase("1")) {
                Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE(), "F");
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE().equalsIgnoreCase("2")) {
                Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE(), "H");
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOPEN_ACCESS_JNL_TYPE_CODE().equalsIgnoreCase("3")) {
                Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE(), "S");
            } else {
                Assert.assertEquals("The OPEN_ACCESS_JNL_TYPE_CODE is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE(), "N");
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_VOLUME_NAME() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_VOLUME_NAME() != null) {
                Assert.assertEquals("The volume is not 0 for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_VOLUME_NAME(), "0");
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_EDITION_NAME() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_EDITION_NAME() != null) {
                assertTrue("Expecting the Edition number details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getBOOK_EDITION_NAME()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_EDITION_NAME()));
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMC() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPMC() != null) {
                assertTrue("Expecting the PMC details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getPMC()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPMC()));
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE() == null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE() == null) {
                Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i),
                        "UNK", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE());
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE().equalsIgnoreCase("STAB") ||
                    dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE().equalsIgnoreCase("SERMEM")) {
                Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE(), "RBK");
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE().equalsIgnoreCase("CONT") ||
                    dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE().equalsIgnoreCase("JNL") ||
                    dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_TYPE().equalsIgnoreCase("CABS")) {
                Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE(), "JNL");
            } else {
                Assert.assertEquals("The Work type is incorrect for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE(), "BKS");
            }


            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_STATUS() == null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS() == null) {
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i),
                        "UNK", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            }else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getRECORD_END_DATE() !=null){
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i),
                        "NVW", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            }else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_SUBSTATUS().equalsIgnoreCase("DIVESTED")) {
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i),
                        "WDV", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_STATUS().equalsIgnoreCase("1")) {
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i),
                        "WLA", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getWORK_STATUS().equalsIgnoreCase("3")) {
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i)
                        , "WAP", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            } else {
                Assert.assertEquals("The Work status is incorrect for id=" + ids.get(i),
                        "WDI", dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS());
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getIMPRINT() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getIMPRINT() != null) {
                assertTrue("Expecting the Imprint details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getIMPRINT()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getIMPRINT()));
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP() != null) {
                if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP().equalsIgnoreCase("EFO")
                        || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP().equalsIgnoreCase("SCAF")
                        || dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP().equalsIgnoreCase("SCCT")) {
                    Assert.assertEquals("The Ownership is incorrect for id=" + ids.get(i)
                            , dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOWNERSHIP(), "ELSOWN");
                } else if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getOWNERSHIP().equalsIgnoreCase("SFO")) {
                    Assert.assertEquals("The Ownership is incorrect for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOWNERSHIP(), "SOCOWN");
                } else {
                    Assert.assertEquals("The Ownership is incorrect for id=" + ids.get(i),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOWNERSHIP(), "COOWN");
                }
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_OPCO_R12() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getF_OPCO_R12() != null) {
                assertTrue("Expecting the OPCO details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_OPCO_R12()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getF_OPCO_R12()));
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_RESPONSIBILITY_CENTRE() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getF_RESPONSIBILITY_CENTRE() != null) {
                assertTrue("Expecting the resp centre details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getF_RESPONSIBILITY_CENTRE()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getF_RESPONSIBILITY_CENTRE()));
            }

            if (dataQualityContext.workDataObjectsFromPMXSTG.get(i).getLANGUAGE_CODE() != null
                    && dataQualityContext.workDataObjectsFromSTGDQ.get(i).getLANGUAGE_CODE() != null) {
                assertTrue("Expecting the Language code details from STG and DQ Consistent for id=" + ids.get(i),
                        dataQualityContext.workDataObjectsFromPMXSTG.get(i).getLANGUAGE_CODE()
                                .equals(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getLANGUAGE_CODE()));
            }
        }
    }

    @And("^The work data between EPH DQ and EPH SA is identical$")
    public void checkDQandSAData() {
        dataQualityContext.workDataObjectsFromSTGDQ.sort(Comparator.comparing(WorkDataObject::getPMX_SOURCE_REFERENCE));
        dataQualityContext.workDataObjectsFromEPH.sort(Comparator.comparing(WorkDataObject::getPMX_SOURCE_REFERENCE));

        if(dataQualityContext.workDataObjectsFromEPH.isEmpty()) {
            Log.info("No data found");
        } else {
            for (int i = 0; i < dataQualityContext.workDataObjectsFromSource.size(); i++) {

                Log.info(dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TITLE());
                Log.info(dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TITLE());
                assertTrue("Expecting the Work title details from DQ and SA Staging Consistent for id=" +
                                dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TITLE()
                                .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TITLE()));

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_SUBTITLE() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_SUBTITLE() != null) {
                    assertTrue("Expecting the WORK subtitle from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_SUBTITLE()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_SUBTITLE()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPRODUCT_WORK_PUB_DATE() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getPRODUCT_WORK_PUB_DATE() != null) {
                    assertTrue("Expecting the WORK pub date from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPRODUCT_WORK_PUB_DATE()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getPRODUCT_WORK_PUB_DATE()));
                }
                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getELECTRONIC_RIGHTS_IND() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getELECTRONIC_RIGHTS_IND() != null) {
                    assertTrue("Expecting the Electronic rights ind details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getELECTRONIC_RIGHTS_IND()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getELECTRONIC_RIGHTS_IND()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_VOLUME_NAME() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_VOLUME_NAME() != null) {
                    assertTrue("Expecting the Volume details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_VOLUME_NAME()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_VOLUME_NAME()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_EDITION_NAME() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_EDITION_NAME() != null) {
                    assertTrue("Expecting the BOOK_EDITION_Number details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getBOOK_EDITION_NAME()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_EDITION_NAME()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null) {
                    assertTrue("Expecting the Open access details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPMC() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getPMC() != null) {
                    assertTrue("Expecting the PMC details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getPMC()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getPMC()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TYPE() != null) {
                    assertTrue("Expecting the Work type details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_TYPE()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TYPE()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_STATUS() != null) {
                    assertTrue("Expecting the Work status details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getWORK_STATUS()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_STATUS()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getIMPRINT() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getIMPRINT() != null) {
                    assertTrue("Expecting the Imprint details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getIMPRINT()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getIMPRINT()));
                }

                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOWNERSHIP() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getOWNERSHIP() != null) {
                    assertTrue("Expecting the Ownership details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getOWNERSHIP()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getOWNERSHIP()));
                }

                assertEquals("Expecting the Acc prod details from DQ and SA Consistent for id="+
                                dataQualityContext.workDataObjectsFromEPH.get(i).getPMX_SOURCE_REFERENCE(),
                        dataQualityContext.workDataObjectsFromSTGDQ.get(i).getF_accountable_product()
                                ,dataQualityContext.workDataObjectsFromEPH.get(i).getF_accountable_product());


                if (dataQualityContext.workDataObjectsFromSTGDQ.get(i).getLANGUAGE_CODE() != null
                        || dataQualityContext.workDataObjectsFromEPH.get(i).getLANGUAGE_CODE() != null) {
                    assertTrue("Expecting the Language code details from DQ and SA Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPH.get(i).getLANGUAGE_CODE(),
                            dataQualityContext.workDataObjectsFromSTGDQ.get(i).getLANGUAGE_CODE()
                                    .equals(dataQualityContext.workDataObjectsFromEPH.get(i).getLANGUAGE_CODE()));
                }
            }
        }
    }

    @And("^The work data between EPH SA and EPH GD is identical$")
    public void checkBetweenSAandGD(){

        dataQualityContext.workDataObjectsFromEPHGD.sort(Comparator.comparing(WorkDataObject::getPMX_SOURCE_REFERENCE));
        dataQualityContext.workDataObjectsFromEPH.sort(Comparator.comparing(WorkDataObject::getPMX_SOURCE_REFERENCE));

        if(dataQualityContext.workDataObjectsFromEPH.isEmpty()) {
            Log.info("No data found");
        } else {
            for (int i = 0; i < dataQualityContext.workDataObjectsFromSource.size(); i++) {
                assertTrue("Expecting the Work title details from SA and GD Staging Consistent for id=" +
                                dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                        dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TITLE()
                                .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TITLE()));

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_SUBTITLE() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_SUBTITLE() != null) {
                    assertTrue("Expecting the Work subtitle details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_SUBTITLE()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_SUBTITLE()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getPRODUCT_WORK_PUB_DATE() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getPRODUCT_WORK_PUB_DATE() != null) {
                    assertTrue("Expecting the Pub date details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getPRODUCT_WORK_PUB_DATE()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPRODUCT_WORK_PUB_DATE()));
                }
                if (dataQualityContext.workDataObjectsFromEPH.get(i).getELECTRONIC_RIGHTS_IND() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getELECTRONIC_RIGHTS_IND() != null) {
                    assertTrue("Expecting the Electronic Rights Ind details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getELECTRONIC_RIGHTS_IND()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getELECTRONIC_RIGHTS_IND()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_VOLUME_NAME() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getBOOK_VOLUME_NAME() != null) {
                    assertTrue("Expecting the BOOK_VOLUME_NAME details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_VOLUME_NAME()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getBOOK_VOLUME_NAME()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_EDITION_NAME() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getBOOK_EDITION_NAME() != null) {
                    assertTrue("Expecting the BOOK_EDITION_NUMBER details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getBOOK_EDITION_NAME()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getBOOK_EDITION_NAME()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getOPEN_ACCESS_JNL_TYPE_CODE() != null) {
                    assertTrue("Expecting the Open access details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getOPEN_ACCESS_JNL_TYPE_CODE()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getPMC() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC() != null) {
                    assertTrue("Expecting the PMC details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getPMC()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMC()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TYPE() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE() != null) {
                    assertTrue("Expecting the Work type details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_TYPE()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_TYPE()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_STATUS() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS() != null) {
                    assertTrue("Expecting the Work status details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getWORK_STATUS()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getWORK_STATUS()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getIMPRINT() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getIMPRINT() != null) {
                    assertTrue("Expecting the Imprint details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getIMPRINT()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getIMPRINT()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getOWNERSHIP() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getOWNERSHIP() != null) {
                    assertTrue("Expecting the Ownership details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getOWNERSHIP()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getOWNERSHIP()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getF_accountable_product() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product() != null) {
                    Log.info(dataQualityContext.workDataObjectsFromEPH.get(i).getF_accountable_product());
                    Log.info(dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product());
                    assertTrue("Expecting the Acc prod details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getPMX_SOURCE_REFERENCE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getF_accountable_product()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getF_accountable_product()));
                }

                if (dataQualityContext.workDataObjectsFromEPH.get(i).getLANGUAGE_CODE() != null
                        || dataQualityContext.workDataObjectsFromEPHGD.get(i).getLANGUAGE_CODE() != null) {
                    assertTrue("Expecting the Language code details from SA and GD Consistent for id=" +
                                    dataQualityContext.workDataObjectsFromEPHGD.get(i).getLANGUAGE_CODE(),
                            dataQualityContext.workDataObjectsFromEPH.get(i).getLANGUAGE_CODE()
                                    .equals(dataQualityContext.workDataObjectsFromEPHGD.get(i).getLANGUAGE_CODE()));
                }
            }
        }
    }
}
