package com.eph.automation.testing.web.steps.specific;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.TestContext;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.ui.SpecificTasks;
import com.google.inject.Inject;
import com.mysql.cj.api.result.Row;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SpecificScenariosSteps {
    //created by Nishant @ 26 Apr 2021

  private SpecificTasks specificTasks;


    @Inject
    public SpecificScenariosSteps(SpecificTasks specificTasks)
    {
        this.specificTasks = specificTasks;
    }







    @Given("^(.*) have a valid file available (.*) to read$")
    public void verifyFileExists(String filePath, String fileName) throws Exception {
        //created by Nishant @ 26 Apr 2021

        String absoluteFilePath = filePath+fileName;
        Assert.assertTrue("valid file exists - ",specificTasks.verifyFileExists(absoluteFilePath));

        //read file as whole
        DataQualityContext.dataFileRowColumn = specificTasks.readCsv(absoluteFilePath);
        System.out.println("Total entries in input datafile - "+DataQualityContext.dataFileRowColumn.size()+"\n");

    }

    @And("^Read file from S3 bucket (.*) and key (.*)$")
    public void readS3File(String bucket,String key) throws Exception {//created by Nishant @ 7 May 2021
       key = TestContext.getValues().s3Key;
       DataQualityContext.dataFileRowColumn =  specificTasks.readS3fileAPI(bucket,key);
       Log.info("headers count for s3 data file - " +DataQualityContext.dataFileRowColumn.get(0).size());

    }



    @Then("^verify links and write result to (.*)$")
    public void verifyLinksFromFile(String FilePath) throws Exception {
        //created by Nishant @ 26 Apr 2021
       int RowFrom = 0; int RowTill = 0;

        if(false) {RowFrom = 1;             RowTill = 6;}//running on local
        else{
            RowFrom = TestContext.getValues().rowFrom;
            RowTill = TestContext.getValues().rowTill;
        Log.info("RowFrom = "+RowFrom);
        Log.info("RowTill = "+RowTill);

        }//running by Jenkins

        if(RowTill==0) RowTill=DataQualityContext.dataFileRowColumn.size();

        DataQualityContext.resultFileName = DataQualityContext.DateAndTime+" Link Testing Result from "+RowFrom+" to "+RowTill+".csv";

       //write headers
        specificTasks.writecsvHeader(FilePath+DataQualityContext.resultFileName);

        //set set counter for specific rows and verify those links
        for(int rowCnt=RowFrom;rowCnt<RowTill;rowCnt++)
        {
            System.out.println("");
          Log.info("...Row count - " + rowCnt);
          DataQualityContext.DataToWrite.add(String.valueOf(rowCnt)); //1st value in result file

          //String[] testData = DataQualityContext.RowData.get(rowCnt).split(",");
          System.out.println(DataQualityContext.dataFileRowColumn.get(rowCnt).get(0));

          DataQualityContext.DataToWrite.add(DataQualityContext.dataFileRowColumn.get(rowCnt).get(0)); //2nd value in result file

            specificTasks.verifySingleLink(DataQualityContext.dataFileRowColumn.get(rowCnt).get(0));

            //write result file
            specificTasks.writeCsv(FilePath+DataQualityContext.resultFileName,DataQualityContext.DataToWrite);

            //reset variable
            specificTasks.resetValues(DataQualityContext.DataToWrite);
        }

      //  specificTasks.uploadToS3("eph-test-data/QA",FilePath+DataQualityContext. resultFileName,DataQualityContext.resultFileName);
    }

    @And ("^upload result (.*) to s3 (.*)$")
    public void uploadToS3(String sourceFilePath,String s3Bucket) throws IOException {
        specificTasks.uploadToS3(s3Bucket,sourceFilePath+DataQualityContext.resultFileName,DataQualityContext.resultFileName);
    }


    @And ("^print file data$")
    public void testtemp()
    {//created by Nishant @ 7 May 2021
        Log.info("other methods can read this data\n");
        for(String colHeaders:DataQualityContext.dataFileRowColumn.get(0))
        {
            System.out.println(colHeaders);
        }
    }

}
