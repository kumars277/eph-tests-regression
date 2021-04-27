package com.eph.automation.testing.web.steps.specific;

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.eph.automation.testing.models.ui.SpecificTasks;
import com.google.inject.Inject;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

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







    @Given("^We have a valid file available (.*) (.*) to read$")
    public void verifyFileExists(String filePath, String fileType) throws Exception {
        //created by Nishant @ 26 Apr 2021

        Assert.assertTrue("valid file exists - ",specificTasks.verifyFileExists(filePath,fileType));

        //read file as whole
        DataQualityContext.RowData = specificTasks.readCsv(filePath+fileType);
        System.out.println("Total entries in input datafile - "+DataQualityContext.RowData.size()+"\n");

    }

    @Then("^verify links from file (.*) (.*)$")
    public void verifyLinksFromFile(String FilePath, String fileType) throws Exception {
        //created by Nishant @ 26 Apr 2021
        int RowFrom = 0;        int RowTill = 10;

        String resultFileName = "\\Result from "+RowFrom+" to "+RowTill+".csv";
        //set set counter for specific rows and verify those links
        for(int rowCnt=RowFrom;rowCnt<RowTill;rowCnt++)
        {
            System.out.println("");
          Log.info("...Row count - " + rowCnt);
          DataQualityContext.DataToWrite.add(String.valueOf(rowCnt)); //1st value in result file

          String[] testData = DataQualityContext.RowData.get(rowCnt).split(",");
          System.out.println(testData[0]);            DataQualityContext.DataToWrite.add(testData[0]); //2nd value in result file
         specificTasks.verifySingleLink(testData[0]);

            //write result file
            specificTasks.writeCsv(FilePath+resultFileName,DataQualityContext.DataToWrite);

            //reset variable
            specificTasks.resetValues(DataQualityContext.DataToWrite);
        }








        //  specificTasks.verifyLinksFromCsv(FilePath+fileType,FilePath);
/*
        //create a callable for each method
        Callable<Void> callable1 = new Callable<Void>(){@Override public Void call() throws Exception{productFinderTasks.verifyLinksFromCsv(FilePath+fileType,FilePath);return null;}};
        Callable<Void> callable2 = new Callable<Void>(){ @Override public Void call() throws Exception{productFinderTasks.verifyLinksFromCsv(FilePath+fileType,FilePath);return null;}};

        //add to a list
        List<Callable<Void>> taskList = new ArrayList<Callable<Void>>();
        taskList.add(callable1);
        taskList.add(callable2);

        //create a pool executor with 3 threads
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try{//start the threads and wait for them to finish
            executor.invokeAll(taskList);}
        catch (InterruptedException ie){        }
*/


    }
}