package com.eph.automation.testing.steps.specific;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.api.WorkApiObject;
import com.eph.automation.testing.models.api.WorksMatchedApiObject;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.ui.SpecificTasks;
import com.eph.automation.testing.services.api.APIService;
import com.eph.automation.testing.services.api.AzureOauthTokenFetchingException;
import com.google.inject.Inject;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class specificScenariosSteps {
  // created by Nishant @ 26 Apr 2021

  private SpecificTasks specificTasks;

  APIService apiService = new APIService();

  @Inject
  public specificScenariosSteps(SpecificTasks specificTasks) {
    this.specificTasks = specificTasks;
  }

  @And("^Read file from S3 bucket (.*) and key (.*)$")
  public void readS3File(String bucket, String key)
      throws Exception { // created by Nishant @ 7 May 2021
    key = TestContext.getValues().s3Key;
    DataQualityContext.dataFileRowColumn = specificTasks.readS3fileAPI(bucket, key);
    Log.info(
        "headers count for s3 data file - " + DataQualityContext.dataFileRowColumn.get(0).size());
  }

  @Then("^verify links and write result to (.*)$")
  public void verifyLinksFromFile(String FilePath) throws Exception {
    // created by Nishant @ 26 Apr 2021
    int RowFrom = 0;
    int RowTill = 0;

    if (false) {
      RowFrom = 1;
      RowTill = 6;
    } // running on local
    else {

      if (TestContext.getValues().rowFrom.equalsIgnoreCase("")) RowFrom = 1;
      else RowFrom = Integer.valueOf(TestContext.getValues().rowFrom);

      if (TestContext.getValues().rowTill.equalsIgnoreCase(""))
        RowTill = DataQualityContext.dataFileRowColumn.size();
      else RowTill = Integer.valueOf(TestContext.getValues().rowTill);
    } // running by Jenkins

    Log.info("RowFrom = " + RowFrom);
    Log.info("RowTill = " + RowTill);

    // if(RowTill==0) RowTill=DataQualityContext.dataFileRowColumn.size();

    DataQualityContext.resultFileName =
        DataQualityContext.DateAndTime
            + " Link Testing Result from "
            + RowFrom
            + " to "
            + RowTill
            + ".csv";

    // write headers
    specificTasks.writecsvHeader(FilePath + DataQualityContext.resultFileName);

    // set set counter for specific rows and verify those links
    for (int rowCnt = RowFrom; rowCnt < RowTill; rowCnt++) {
      System.out.println("");
      Log.info("...Row count - " + rowCnt);
      DataQualityContext.DataToWrite.add(String.valueOf(rowCnt)); // 1st value in result file

      // String[] testData = DataQualityContext.RowData.get(rowCnt).split(",");
      System.out.println(DataQualityContext.dataFileRowColumn.get(rowCnt).get(0));

      DataQualityContext.DataToWrite.add(
          DataQualityContext.dataFileRowColumn.get(rowCnt).get(0)); // 2nd value in result file

      specificTasks.verifySingleLink(DataQualityContext.dataFileRowColumn.get(rowCnt).get(0));

      // write result file
      specificTasks.writeCsv(
          FilePath + DataQualityContext.resultFileName, DataQualityContext.DataToWrite);

      // reset variable
      specificTasks.resetValues(DataQualityContext.DataToWrite);
    }

    //  specificTasks.uploadToS3("eph-test-data/QA",FilePath+DataQualityContext.
    // resultFileName,DataQualityContext.resultFileName);
  }

  @And("^upload result (.*) to s3 (.*)$")
  public void uploadToS3(String sourceFilePath, String s3Bucket) throws IOException {
    specificTasks.uploadToS3(
        s3Bucket,
        sourceFilePath + DataQualityContext.resultFileName,
        DataQualityContext.resultFileName);
  }

  @Given("^(.*) have a valid file available (.*) to read$")
  public void verifyFileExists(String filePath, String fileName) throws Exception {
    // created by Nishant @ 26 Apr 2021

    String absoluteFilePath = filePath + fileName;
    Assert.assertTrue("valid file exists - ", specificTasks.verifyFileExists(absoluteFilePath));

    // read file as whole
    DataQualityContext.dataFileRowColumn = specificTasks.readCsv(absoluteFilePath);
    System.out.println(
        "Total entries in input datafile - " + DataQualityContext.dataFileRowColumn.size() + "\n");
  }

  @Given("^get create relationship hirarchy (.*)$")
  public void createRelationshipHirarchy(String maniIsbn) {
    try {
      List<Map<?, ?>> randomWorkId = getRandomWorkWithEdi(10);

      WorksMatchedApiObject returnedWorks = null;
      List<String> workId = new ArrayList<>();

      for (int i = 0; i < randomWorkId.size(); i++) {

        returnedWorks =
            apiService.getWorksBySearchOption(
                randomWorkId.get(i).get("f_parent").toString());

        returnedWorks.printWorkEdition();

        workId.add(returnedWorks.getWorkEdition());
        while (!workId.isEmpty()) {
          workId = getParentRelationship(workId);
        }

        workId.add(returnedWorks.getWorkEdition());
        while (!workId.isEmpty()) {
          workId = getChildRelationship(workId);
        }
      }

    } catch (AzureOauthTokenFetchingException e) {
      e.printStackTrace();
    }
  }

  @And("^print file data$")
  public void testtemp() { // created by Nishant @ 7 May 2021
    Log.info("other methods can read this data\n");
    for (String colHeaders : DataQualityContext.dataFileRowColumn.get(0)) {
      System.out.println(colHeaders);
    }
  }

  public List<Map<?, ?>> getRandomWorkWithEdi(int noOfId) {
    String sql =
        "select f_parent from semarchy_eph_mdm.gd_work_relationship where f_relationship_type ='EDI' order by random() limit "
            + noOfId;

    List<Map<?, ?>> workId = DBManager.getDBResultMap(sql, Constants.EPH_URL);
    return workId;
  }

  public List<String> getParentRelationship(List<String> workId) {
    List<String> parents = new ArrayList<>();

    for (int itr = 0; itr < workId.size(); itr++) {
      try {
        WorkApiObject workApi_response;
        String sql =
            "select gwr.f_parent ,gwr.f_child , gwr.f_relationship_type, \n"
                + "gxlrt.code,gxlrt.parent_description, gxlrt.child_description , gxlrt.l_description\n"
                + "from semarchy_eph_mdm.gd_work_relationship gwr,\n"
                + "semarchy_eph_mdm.gd_x_lov_relationship_type gxlrt\n"
                + "where f_child in('"
                + workId.get(itr)
                + "')\n"
                + "and gxlrt.code =gwr.f_relationship_type";

        List<Map<?, ?>> relationship = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        for (int i = 0; i < relationship.size(); i++) {
          if (relationship.get(i).get("code").toString().equalsIgnoreCase("EDI")) {
            String parentWork = relationship.get(i).get("f_parent").toString();
            workApi_response = apiService.getWorkByID(parentWork);

            System.out.println(
                "\nparent relationship"
                    + "\nRelationship Type : "
                    + relationship.get(i).get("f_relationship_type").toString()
                    + "\n          work ID : "
                    + workApi_response.getId()
                    + "\n      Master ISBN : "
                    + workApi_response.getWorkExtended().getMasterISBN()
                    + "\n      Edition no. : "
                    + workApi_response.getWorkCore().getEditionNumber()
                    + "\n         Child Id : "
                    + workId.get(itr)
                    + "\n");

            parents.add(workApi_response.getId());
          }
        }

      } catch (AzureOauthTokenFetchingException e) {
        e.printStackTrace();
      }
    }
    return parents;
  }

  public List<String> getChildRelationship(List<String> workId) {
    List<String> children = new ArrayList<>();

    for (int itr = 0; itr < workId.size(); itr++) {
      try {
        WorkApiObject workApi_response;
        String sql =
            "select gwr.f_parent ,gwr.f_child , gwr.f_relationship_type, \n"
                + "gxlrt.code,gxlrt.parent_description, gxlrt.child_description , gxlrt.l_description\n"
                + "from semarchy_eph_mdm.gd_work_relationship gwr,\n"
                + "semarchy_eph_mdm.gd_x_lov_relationship_type gxlrt\n"
                + "where f_parent in ('"
                + workId.get(itr)
                + "')\n"
                + "and gxlrt.code =gwr.f_relationship_type";

        List<Map<?, ?>> relationship = DBManager.getDBResultMap(sql, Constants.EPH_URL);

        for (int i = 0; i < relationship.size(); i++) {
          if (relationship.get(i).get("code").toString().equalsIgnoreCase("EDI")) {
            String childWork = relationship.get(i).get("f_child").toString();
            workApi_response = apiService.getWorkByID(childWork);

            System.out.println(
                "\nchild relationship"
                    + "\nRelationship Type : "
                    + relationship.get(i).get("f_relationship_type").toString()
                    + "\n          work ID : "
                    + workApi_response.getId()
                    + "\n      Master ISBN : "
                    + workApi_response.getWorkExtended().getMasterISBN()
                    + "\n      Edition no. : "
                    + workApi_response.getWorkCore().getEditionNumber()
                    + "\n         Parent Id : "
                    + workId.get(itr)
                    + "\n");
            children.add(workApi_response.getId());
          }
        }

      } catch (AzureOauthTokenFetchingException e) {
        e.printStackTrace();
      }
    }
    return children;
  }
}
