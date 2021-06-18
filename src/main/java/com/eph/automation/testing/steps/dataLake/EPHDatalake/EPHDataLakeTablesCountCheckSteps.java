package com.eph.automation.testing.steps.dataLake.EPHDatalake;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.services.db.EPHDataLakeSql.*;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import org.junit.Assert;

import java.util.List;
import java.util.Map;


public class EPHDataLakeTablesCountCheckSteps {

    private static String sqlDL;
    private static String sqlEPH;
    private static int DLCount;
    private static int EPHCount;


// gd_Wwork count
    @Given("^We know the number of Works in EPH$")
    public void getEph_GD_Works_Count() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_WORK_COUNT;

        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEph = DBManager.getDBResultMap(sqlEPH,Constants.EPH_URL);
        EPHCount = ((Long) workCountEph.get(0).get("EPH_gd_Wwork_Count")).intValue();
        Log.info("GD Works in EPH are: " + EPHCount);
    }

    @When("^The wwork data is in the DL$")
    public void getDL_GD_Works_Count() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_WORK_COUNT;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Wwork_Count")).intValue();
        Log.info("GD Works in DL staging: " + DLCount);

    }

//gd Accountable product count
    @Given("^We know the number of Accountable products in EPH$")
    public void getEPH_GD_Accountable_Product() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Accountable_Product_Count;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Accountable_Product_Count")).intValue();
        Log.info("GD Accountable_Product in EPH Count: " + EPHCount);
    }

    @When("^The Accountable product data is in the DL$")
    public void getDL_GD_Accountable_Product() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Accountable_Product_Count;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Accountable_Product_Count")).intValue();
        Log.info("GD Accountable_Product in DL: " + DLCount);
    }

    //gd event count
    @Given("^We know the number of event data in EPH$")
    public void getEPH_GD_Event() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Event;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Event")).intValue();
        Log.info("GD Event data in EPH Count: " + EPHCount);
    }

    @When("^The event data is in the DL$")
    public void getDL_GD_Event() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Event;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Event")).intValue();
        Log.info("GD Event data in DL: " + DLCount);
    }

    //gd manifestation count
    @Given("^We know the number of manifestation data in EPH$")
    public void getEPH_GD_Manifestation() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Manifestation;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Manifestation")).intValue();
        Log.info("GD Manifestation data in EPH Count: " + EPHCount);
    }

    @When("^The manifestation data is in the DL$")
    public void getDL_GD_Manifestation() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Manifestation;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Manifestation")).intValue();
        Log.info("GD Manifestation data in DL: " + DLCount);
    }

    //gd manifestation_identifier count
    @Given("^We know the number of manifestation_identifier data in EPH$")
    public void getEPH_GD_Manifestation_Identifier() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Manifestation_Identifier;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Manifestation_Identifier")).intValue();
        Log.info("GD Manifestation data in EPH Count: " + EPHCount);
    }

    @When("^The manifestation_identifier data is in the DL$")
    public void getDL_GD_Manifestation_Identifier() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Manifestation_Identifier;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Manifestation_Identifier")).intValue();
        Log.info("GD Manifestation_Identifier data in DL: " + DLCount);
    }

    //gd person count
    @Given("^We know the number of person data in EPH$")
    public void getEPH_GD_Person() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Person;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Person")).intValue();
        Log.info("GD Person data in EPH Count: " + EPHCount);
    }

    @When("^The person data is in the DL$")
    public void getDL_GD_Person() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Person;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Person")).intValue();
        Log.info("GD Person data in DL: " + DLCount);
    }

    //gd product count
    @Given("^We know the number of product data in EPH$")
    public void getEPH_GD_Product() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Product;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Product")).intValue();
        Log.info("GD Product data in EPH Count: " + EPHCount);
    }

    @When("^The product data is in the DL$")
    public void getDL_GD_Product() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Product;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Product")).intValue();
        Log.info("GD Product data in DL: " + DLCount);
    }

    //gd product person role count
    @Given("^We know the number of product person role data in EPH$")
    public void getEPH_GD_Product_Person_Role() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Product_Person_Role;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Product_Person_Role")).intValue();
        Log.info("GD Product person role data in EPH Count: " + EPHCount);
    }

    @When("^The product person role data is in the DL$")
    public void getDL_GD_Product_Person_Role() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Product_Person_Role;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Product_Person_Role")).intValue();
        Log.info("GD Product person role data in DL: " + DLCount);
    }

    //gd product rel package count
    @Given("^We know the number of product rel package data in EPH$")
    public void getEPH_GD_Product_Rel_Package() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Product_Rel_Package;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Product_Rel_Package")).intValue();
        Log.info("GD Product rel package data in EPH Count: " + EPHCount);
    }

    @When("^The product rel package data is in the DL$")
    public void getDL_GD_Product_Rel_Package() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Product_Rel_Package;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Product_Rel_Package")).intValue();
        Log.info("GD Product rel package data in DL: " + DLCount);
    }

    //gd subject area
    @Given("^We know the number of subject area data in EPH$")
    public void getEPH_GD_Subject_Area() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Subject_Area;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Subject_Area")).intValue();
        Log.info("GD Subject Area in EPH Count: " + EPHCount);
    }

    @When("^The subject area data is in the DL$")
    public void getDL_GD_Subject_Area() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Subject_Area;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Subject_Area")).intValue();
        Log.info("GD Subject Area data in DL: " + DLCount);
    }

    //gd work financial attributes
    @Given("^We know the number of work financial attribs data in EPH$")
    public void getEPH_GD_Work_Financial_Attribs() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Work_Financial_Attribs;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Work_Financial_Attribs")).intValue();
        Log.info("GD Work Financial Attribs in EPH Count: " + EPHCount);
    }

    @When("^The work financial attribs data is in the DL$")
    public void getDL_GD_Work_Financial_Attribs() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Work_Financial_Attribs;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Work_Financial_Attribs")).intValue();
        Log.info("GD Work_Financial_Attribs data in DL: " + DLCount);
    }

    //gd work financial attributes
    @Given("^We know the number of work identifier data in EPH$")
    public void getEPH_GD_Work_Identifier() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Work_Identifier;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Work_Identifier")).intValue();
        Log.info("GD Work Identifier in EPH Count: " + EPHCount);
    }

    @When("^The work identifier data is in the DL$")
    public void getDL_GD_Work_Identifier() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Work_Identifier;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Work_Identifier")).intValue();
        Log.info("GD Work Identifier data in DL: " + DLCount);
    }

    //gd work person role
    @Given("^We know the number of work person role data in EPH$")
    public void getEPH_GD_Work_Person_Role() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Work_Person_Role;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Work_Person_Role")).intValue();
        Log.info("GD Work Person Role in EPH Count: " + EPHCount);
    }

    @When("^The work person role data is in the DL$")
    public void getDL_GD_Work_Person_Role() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Work_Person_Role;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Work_Person_Role")).intValue();
        Log.info("GD Work Person Role data in DL: " + DLCount);
    }


    //gd work relationship
    @Given("^We know the number of work relationship data in EPH$")
    public void getEPH_GD_Work_Relationship() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Work_Relationship;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Work_Relationship")).intValue();
        Log.info("GD Work Relationship in EPH Count: " + EPHCount);
    }

    @When("^The work relationship data is in the DL$")
    public void getDL_GD_Work_Relationship() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Work_Relationship;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Work_Relationship")).intValue();
        Log.info("GD Work Relationship data in DL: " + DLCount);
    }

    //gd work subject area link
    @Given("^We know the number of work subject area link data in EPH$")
    public void getEPH_GD_Work_Subject_Area_Link() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_Work_Subject_Area_Link;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_Work_Subject_Area_Link")).intValue();
        Log.info("GD Work Subject Area Link in EPH Count: " + EPHCount);
    }

    @When("^The work subject area link data is in the DL$")
    public void getDL_GD_Work_Subject_Area_Link() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_Work_Subject_Area_Link;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_Work_Subject_Area_Link")).intValue();
        Log.info("GD Work Subject Area Link data in DL: " + DLCount);
    }

    //gd x lov etax product code
    @Given("^We know the number of x lov etax product code data in EPH$")
    public void getEPH_GD_X_Lov_Etax_Product_Code() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Etax_Product_Code;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Etax_Product_Code")).intValue();
        Log.info("GD X lov etax product code in EPH Count: " + EPHCount);
    }

    @When("^The x lov etax product code data is in the DL$")
    public void getDL_GD_X_Lov_Etax_Product_Code() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Etax_Product_Code;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Etax_Product_Code")).intValue();
        Log.info("GD X Lov Etax Product Code data in DL: " + DLCount);
    }

    //gd x lov event type
    @Given("^We know the number of x lov event type data in EPH$")
    public void getEPH_GD_X_Lov_Event_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Event_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Event_Type")).intValue();
        Log.info("GD X lov event type in EPH Count: " + EPHCount);
    }

    @When("^The x lov event type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Event_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Event_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Event_Type")).intValue();
        Log.info("GD X Lov Event Type data in DL: " + DLCount);
    }

    //gd x lov gl company
    @Given("^We know the number of x lov gl company data in EPH$")
    public void getEPH_GD_X_Lov_Gl_Company() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Gl_Company;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Gl_Company")).intValue();
        Log.info("GD X lov gl company in EPH Count: " + EPHCount);
    }

    @When("^The x lov gl company data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Gl_Company() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Gl_Company;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Gl_Company")).intValue();
        Log.info("GD X Lov gl company data in DL: " + DLCount);
    }

    //gd x lov gl prod seg parent
    @Given("^We know the number of x lov gl prod seg parent data in EPH$")
    public void getEPH_GD_X_Lov_Gl_Prod_Seg_Parent() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Gl_Prod_Seg_Parent;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Gl_Prod_Seg_Parent")).intValue();
        Log.info("GD X lov gl prod seg parent in EPH Count: " + EPHCount);
    }

    @When("^The x lov gl prod seg parent data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Gl_Prod_Seg_Parent() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Gl_Prod_Seg_Parent;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Gl_Prod_Seg_Parent")).intValue();
        Log.info("GD X Lov gl prod seg parent data in DL: " + DLCount);
    }

    //gd x lov gl prod seg parent
    @Given("^We know the number of x lov gl resp centre data in EPH$")
    public void getEPH_GD_X_Lov_Gl_Resp_Centre() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Gl_Resp_Centre;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Gl_Resp_Centre")).intValue();
        Log.info("GD X lov gl resp centre in EPH Count: " + EPHCount);
    }

    @When("^The x lov gl resp centre data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Gl_Resp_Centre() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Gl_Resp_Centre;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Gl_Resp_Centre")).intValue();
        Log.info("GD X Lov gl resp centre data in DL: " + DLCount);
    }

    //gd x lov gl prod seg parent
    @Given("^We know the number of x lov identifier type data in EPH$")
    public void getEPH_GD_X_Lov_Identifier_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Identifier_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Identifier_Type")).intValue();
        Log.info("GD X lov identifier type in EPH Count: " + EPHCount);
    }

    @When("^The x lov identifier type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Identifier_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Identifier_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Identifier_Type")).intValue();
        Log.info("GD X Lov identifier type data in DL: " + DLCount);
    }

    //gd x lov imprint
    @Given("^We know the number of x lov imprint data in EPH$")
    public void getEPH_GD_X_Lov_Imprint() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Imprint;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Imprint")).intValue();
        Log.info("GD X lov imprint in EPH Count: " + EPHCount);
    }

    @When("^The x lov imprint data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Imprint() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Imprint;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Imprint")).intValue();
        Log.info("GD X Lov imprint data in DL: " + DLCount);
    }

    //gd x lov language
    @Given("^We know the number of x lov language data in EPH$")
    public void getEPH_GD_X_Lov_Language() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Language;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Language")).intValue();
        Log.info("GD X lov language in EPH Count: " + EPHCount);
    }

    @When("^The x lov language data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Language() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Language;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Language")).intValue();
        Log.info("GD X Lov language data in DL: " + DLCount);
    }

    //gd x lov manif status
    @Given("^We know the number of x lov manif status data in EPH$")
    public void getEPH_GD_X_Lov_Manif_Status() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Manif_Status;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Manif_Status")).intValue();
        Log.info("GD X lov manif status in EPH Count: " + EPHCount);
    }

    @When("^The x lov manif status data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Manif_Status() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Manif_Status;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Manif_Status")).intValue();
        Log.info("GD X Lov manif status data in DL: " + DLCount);
    }

    //gd x lov manif type
    @Given("^We know the number of x lov manif type data in EPH$")
    public void getEPH_GD_X_Lov_Manif_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Manif_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Manif_Type")).intValue();
        Log.info("GD X lov manif type in EPH Count: " + EPHCount);
    }

    @When("^The x lov manif type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Manif_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Manif_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Manif_Type")).intValue();
        Log.info("GD X Lov manif type data in DL: " + DLCount);
    }

    //gd x lov oa type
    @Given("^We know the number of x lov oa type data in EPH$")
    public void getEPH_GD_X_Lov_OA_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_OA_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_OA_Type")).intValue();
        Log.info("GD X lov oa type in EPH Count: " + EPHCount);
    }

    @When("^The x lov oa type data is in the DL$")
    public void getDL_GD_X_Lov_OA_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_OA_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_OA_Type")).intValue();
        Log.info("GD X Lov oa type data in DL: " + DLCount);
    }

    //gd x lov oa type
    @Given("^We know the number of x lov origin data in EPH$")
    public void getEPH_GD_X_Lov_Origin() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Origin;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Origin")).intValue();
        Log.info("GD X lov Origin in EPH Count: " + EPHCount);
    }

    @When("^The x lov origin data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Origin() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Origin;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Origin")).intValue();
        Log.info("GD X Lov origin data in DL: " + DLCount);
    }

    //gd x lov person role
    @Given("^We know the number of x lov person role data in EPH$")
    public void getEPH_GD_X_Lov_Person_Role() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Person_Role;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Person_Role")).intValue();
        Log.info("GD X lov Person Role in EPH Count: " + EPHCount);
    }

    @When("^The x lov person role data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Person_Role() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Person_Role;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Person_Role")).intValue();
        Log.info("GD X Lov person role data in DL: " + DLCount);
    }

    //gd x lov pmc
    @Given("^We know the number of x lov pmc data in EPH$")
    public void getEPH_GD_X_Lov_PMC() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_PMC;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_PMC")).intValue();
        Log.info("GD X lov PMC in EPH Count: " + EPHCount);
    }

    @When("^The x lov pmc data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_PMC() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_PMC;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_PMC")).intValue();
        Log.info("GD X Lov pmc data in DL: " + DLCount);
    }

    //gd x lov pmg
    @Given("^We know the number of x lov pmg data in EPH$")
    public void getEPH_GD_X_Lov_PMG() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_PMG;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_PMG")).intValue();
        Log.info("GD X lov PMG in EPH Count: " + EPHCount);
    }

    @When("^The x lov pmg data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_PMG() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_PMG;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_PMG")).intValue();
        Log.info("GD X Lov pmg data in DL: " + DLCount);
    }

    //gd x lov product status
    @Given("^We know the number of x lov product status data in EPH$")
    public void getEPH_GD_X_Lov_Product_Status() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Product_Status;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Product_Status")).intValue();
        Log.info("GD X lov Product Status in EPH Count: " + EPHCount);
    }

    @When("^The x lov product status data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Product_Status() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Product_Status;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Product_Status")).intValue();
        Log.info("GD X Lov product status data in DL: " + DLCount);
    }

    //gd x lov product type
    @Given("^We know the number of x lov product type data in EPH$")
    public void getEPH_GD_X_Lov_Product_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Product_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Product_Type")).intValue();
        Log.info("GD X lov Product Type in EPH Count: " + EPHCount);
    }

    @When("^The x lov product type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Product_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Product_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Product_Type")).intValue();
        Log.info("GD X Lov product type data in DL: " + DLCount);
    }

    //gd x lov relationship type
    @Given("^We know the number of x lov relationship type data in EPH$")
    public void getEPH_GD_X_Lov_Relationship_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Relationship_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Relationship_Type")).intValue();
        Log.info("GD X lov Relationship Type in EPH Count: " + EPHCount);
    }

    @When("^The x lov relationship type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Relationship_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Relationship_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Relationship_Type")).intValue();
        Log.info("GD X Lov relationship type data in DL: " + DLCount);
    }

    //gd x lov revenue model
    @Given("^We know the number of x lov revenue model data in EPH$")
    public void getEPH_GD_X_Lov_Revenue_Model() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Revenue_Model;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Revenue_Model")).intValue();
        Log.info("GD X lov revenue model in EPH Count: " + EPHCount);
    }

    @When("^The x lov revenue model data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Revenue_Model() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Revenue_Model;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Revenue_Model")).intValue();
        Log.info("GD X Lov revenue model data in DL: " + DLCount);
    }

    //gd x lov society ownership
    @Given("^We know the number of x lov society ownership data in EPH$")
    public void getEPH_GD_X_Lov_Society_Ownership() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Society_Ownership;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Society_Ownership")).intValue();
        Log.info("GD X lov society ownership in EPH Count: " + EPHCount);
    }

    @When("^The x lov society ownership data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Society_Ownership() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Society_Ownership;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Society_Ownership")).intValue();
        Log.info("GD X Lov society ownership data in DL: " + DLCount);
    }

    //gd x lov subject area type
    @Given("^We know the number of x lov subject area type data in EPH$")
    public void getEPH_GD_X_Lov_Subject_Area_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Subject_Area_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Subject_Area_Type")).intValue();
        Log.info("GD X lov subject area type in EPH Count: " + EPHCount);
    }

    @When("^The x lov subject area type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Subject_Area_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Subject_Area_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Subject_Area_Type")).intValue();
        Log.info("GD X Lov subject area type data in DL: " + DLCount);
    }

    //gd x lov subscription type
    @Given("^We know the number of x lov subscription type data in EPH$")
    public void getEPH_GD_X_Lov_Subscription_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Subscription_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Subscription_Type")).intValue();
        Log.info("GD X lov Subscription type in EPH Count: " + EPHCount);
    }

    @When("^The x lov subscription type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Subscription_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Subscription_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Subscription_Type")).intValue();
        Log.info("GD X Lov Subscription type data in DL: " + DLCount);
    }

    //gd x lov work status
    @Given("^We know the number of x lov work status data in EPH$")
    public void getEPH_GD_X_Lov_Work_Status() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Work_Status;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Work_Status")).intValue();
        Log.info("GD X lov work status in EPH Count: " + EPHCount);
    }

    @When("^The x lov work status data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Work_Status() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Work_Status;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Work_Status")).intValue();
        Log.info("GD X Lov work status data in DL: " + DLCount);
    }

    //gd x lov work type
    @Given("^We know the number of x lov work type data in EPH$")
    public void getEPH_GD_X_Lov_Work_Type() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Work_Type;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Work_Type")).intValue();
        Log.info("GD X lov work type in EPH Count: " + EPHCount);
    }

    @When("^The x lov work type data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Work_Type() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Work_Type;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Work_Type")).intValue();
        Log.info("GD X Lov work type data in DL: " + DLCount);
    }

    //gd x lov workflow source
    @Given("^We know the number of x lov workflow source data in EPH$")
    public void getEPH_GD_X_Lov_Workflow_Source() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GD_X_Lov_Workflow_Source;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gd_X_Lov_Workflow_Source")).intValue();
        Log.info("GD X lov workflow source in EPH Count: " + EPHCount);
    }

    @When("^The x lov workflow source data is in the DL$")
    public void getDL_GD_X_Lov_X_Lov_Workflow_Source() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GD_X_Lov_Workflow_Source;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gd_X_Lov_Workflow_Source")).intValue();
        Log.info("GD X Lov workflow source data in DL: " + DLCount);
    }

    //gh accountable product
    @Given("^We know the number of gh accountable product data in EPH$")
    public void getEPH_GH_Accountable_Product() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Accountable_Product;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Accountable_Product")).intValue();
        Log.info("GH Accountable Product in EPH Count: " + EPHCount);
    }

    @When("^The gh accountable product data is in the DL$")
    public void getDL_GH_Accountable_Product() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Accountable_Product;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Accountable_Product")).intValue();
        Log.info("GH accountable product data in DL: " + DLCount);
    }

    //gh manifestation
    @Given("^We know the number of gh manifestation data in EPH$")
    public void getEPH_GH_Manifestation() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Manifestation;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Manifestation")).intValue();
        Log.info("GH Manifestation in EPH Count: " + EPHCount);
    }

    @When("^The gh manifestation data is in the DL$")
    public void getDL_GH_Manifestation() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Manifestation;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Manifestation")).intValue();
        Log.info("GH manifestation data in DL: " + DLCount);
    }

    //gh manifestation identifier
    @Given("^We know the number of gh manifestation identifier data in EPH$")
    public void getEPH_GH_Manifestation_Identifier() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Manifestation_Identifier;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Manifestation_Identifier")).intValue();
        Log.info("GH Manifestation identifier in EPH Count: " + EPHCount);
    }

    @When("^The gh manifestation identifier data is in the DL$")
    public void getDL_GH_Manifestation_Identifier() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Manifestation_Identifier;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Manifestation_Identifier")).intValue();
        Log.info("GH manifestation identifier data in DL: " + DLCount);
    }

    //gh person
    @Given("^We know the number of gh person data in EPH$")
    public void getEPH_GH_Person() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Person;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Person")).intValue();
        Log.info("GH Person in EPH Count: " + EPHCount);
    }

    @When("^The gh person data is in the DL$")
    public void getDL_GH_Person() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Person;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Person")).intValue();
        Log.info("GH person data in DL: " + DLCount);
    }

    //gh product
    @Given("^We know the number of gh product data in EPH$")
    public void getEPH_GH_Product() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Product;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Product")).intValue();
        Log.info("GH Product in EPH Count: " + EPHCount);
    }

    @When("^The gh product data is in the DL$")
    public void getDL_GH_Product() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Product;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Product")).intValue();
        Log.info("GH product data in DL: " + DLCount);
    }

    //gh product person role
    @Given("^We know the number of gh product person role data in EPH$")
    public void getEPH_GH_Product_Person_Role() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Product_Person_Role;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Product_Person_Role")).intValue();
        Log.info("GH Product person role in EPH Count: " + EPHCount);
    }

    @When("^The gh product person role data is in the DL$")
    public void getDL_GH_Product_Person_Role() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Product_Person_Role;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Product_Person_Role")).intValue();
        Log.info("GH product person role data in DL: " + DLCount);
    }

    //gh product rel package
    @Given("^We know the number of gh product rel package data in EPH$")
    public void getEPH_GH_Product_Rel_Package() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Product_Rel_Package;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Product_Rel_Package")).intValue();
        Log.info("GH Product rel package in EPH Count: " + EPHCount);
    }

    @When("^The gh product rel package data is in the DL$")
    public void getDL_GH_Product_Rel_Package() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Product_Rel_Package;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Product_Rel_Package")).intValue();
        Log.info("GH product rel package data in DL: " + DLCount);
    }

    //gh work financial attribs
    @Given("^We know the number of gh work financial attribs data in EPH$")
    public void getEPH_GH_Work_Financial_Attribs() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Work_Financial_Attribs;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Work_Financial_Attribs")).intValue();
        Log.info("GH work financial attribs in EPH Count: " + EPHCount);
    }

    @When("^The gh work financial attribs data is in the DL$")
    public void getDL_GH_Work_Financial_Attribs() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Work_Financial_Attribs;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Work_Financial_Attribs")).intValue();
        Log.info("GH work financial attribs data in DL: " + DLCount);
    }

    //gh work identifier
    @Given("^We know the number of gh work identifier data in EPH$")
    public void getEPH_GH_Work_Identifier() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Work_Identifier;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Work_Identifier")).intValue();
        Log.info("GH work identifier in EPH Count: " + EPHCount);
    }

    @When("^The gh work identifier data is in the DL$")
    public void getDL_GH_Work_Identifier() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Work_Identifier;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Work_Identifier")).intValue();
        Log.info("GH work identifier data in DL: " + DLCount);
    }

    //gh work person role
    @Given("^We know the number of gh work person role data in EPH$")
    public void getEPH_GH_Work_Person_Role() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Work_Person_Role;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Work_Person_Role")).intValue();
        Log.info("GH work person role in EPH Count: " + EPHCount);
    }

    @When("^The gh work person role data is in the DL$")
    public void getDL_GH_Work_Person_Role() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Work_Person_Role;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Work_Person_Role")).intValue();
        Log.info("GH work person role data in DL: " + DLCount);
    }

    //gh work relationship
    @Given("^We know the number of gh work relationship data in EPH$")
    public void getEPH_GH_Work_Relationship() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Work_Relationship;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Work_Relationship")).intValue();
        Log.info("GH work relationship in EPH Count: " + EPHCount);
    }

    @When("^The gh work relationship data is in the DL$")
    public void getDL_GH_Work_Relationship() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Work_Relationship;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Work_Relationship")).intValue();
        Log.info("GH work relationship data in DL: " + DLCount);
    }

    //gh work subject area link
    @Given("^We know the number of gh work subject area link data in EPH$")
    public void getEPH_GH_Work_Subject_Area_Link() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Work_Subject_Area_Link;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Work_Subject_Area_Link")).intValue();
        Log.info("GH work subject area link in EPH Count: " + EPHCount);
    }

    @When("^The gh work subject area link data is in the DL$")
    public void getDL_GH_Work_Subject_Area_Link() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Work_Subject_Area_Link;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Work_Subject_Area_Link")).intValue();
        Log.info("GH work subject area link data in DL: " + DLCount);
    }

    //gh Wwork
    @Given("^We know the number of gh wwork data in EPH$")
    public void getEPH_GH_Wwork() {
        sqlEPH = EPHDataLakeTablesCountSQL.EPH_GH_Wwork;
        Log.info(sqlEPH);
        List<Map<String, Object>> workCountEPH = DBManager.getDBResultMap(sqlEPH,
                Constants.EPH_URL);
        EPHCount = ((Long) workCountEPH.get(0).get("EPH_gh_Wwork")).intValue();
        Log.info("GH Wwork in EPH Count: " + EPHCount);
    }

    @When("^The gh wwork data is in the DL$")
    public void getDL_GH_Wwork() {
        sqlDL = EPHDataLakeTablesCountSQL.DL_GH_Wwork;
        Log.info(sqlDL);
        List<Map<String, Object>> workCountDL = DBManager.getDLResultMap(sqlDL,
                Constants.AWS_URL);
        DLCount = ((Long) workCountDL.get(0).get("DL_gh_Wwork")).intValue();
        Log.info("GH Wwork data in DL: " + DLCount);
    }

    //Assertion for all count checks
    @Then("^The count between (.*) and (.*) are identical$")
    public void compareWorkCountEPHtoDL(String source, String target) {
                Assert.assertEquals("The counts between " + source + " and " + target + " is not equal", EPHCount, DLCount);
                Log.info("The count between between " + source + ": " + EPHCount + " and " + target + ": " + DLCount + " is equal");
    }
}



