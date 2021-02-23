package com.eph.automation.testing.web.steps.ExtendedTableToStitchingExtTables;

import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.StitchingExtContext;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtJsonObject;
import com.eph.automation.testing.models.dao.StitchingExtended.WorkExtAccessObject;
import com.eph.automation.testing.services.db.StitchingExtendedSQL.StitchingExtDataChecksSQL;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class WorkExtDataChecksSteps {

    public StitchingExtContext dataQualityStitchContext;
    private static String sql;
    private static List<String> Ids;
    private StitchingExtDataChecksSQL StchObj = new StitchingExtDataChecksSQL();

    // private SimpleDateFormat formatter1=new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
    // private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");



    @Given("^We get the (.*) random work Ext EPR ids (.*)$")
    public void getRandomManifExtEPRIds(String numberOfRecords, String tableName) {
        //numberOfRecords = System.getProperty("dbRandomRecordsNumber"); //Uncomment when running in jenkins
        Log.info("numberOfRecords = " + numberOfRecords);
        Log.info("Get random Work Ext EPR Ids...");
        switch (tableName) {
            case "work_extended":
                sql = String.format(StitchingExtDataChecksSQL.GET_RANDOM_EPR_WORK_EXTENDED, numberOfRecords);
                List<Map<?, ?>> randomWorkExtendedEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
                Ids = randomWorkExtendedEPRIds.stream().map(m -> (String) m.get("epr_id")).collect(Collectors.toList());
                break;

        }
        Log.info(sql);
        Log.info(Ids.toString());
    }

    @Then("^Get the records from Work extended table$")
    public void getRecordsFromWorkExtendedTable() {
        Log.info("We get the records from Work Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_REC, Joiner.on("','").join(Ids));
        Log.info(sql);
        dataQualityStitchContext.recordsFromWorkExtended = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.AWS_URL);
    }

    @Then("^Get the records from Work extended stitching table$")
    public void getWorkExtendedJSONRec(String workId) {
        Log.info("We get the JSON from Work Ext Stitching...");
        Log.info(workId);
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_JSON_REC, workId);
        Log.info(sql);
        List<Map<String, String>> jsonValue = DBManager.getDBResultMap(sql, Constants.EPH_URL);
        StitchingExtContext.recordsFromWorkStitching = new Gson().fromJson(jsonValue.get(0).get("json"), WorkExtJsonObject.class);
    }

    public void getWorkType(String workId){
        Log.info("We get the type from Work Stitching Extended Tables...");
        sql = String.format(StitchingExtDataChecksSQL.GET_WORK_EXT_JSON_REC, workId);
        Log.info(sql);
        dataQualityStitchContext.recFromWorkTypeStitchExtended = DBManager.getDBResultAsBeanList(sql, WorkExtAccessObject.class, Constants.EPH_URL);
    }

    @And("^Compare work Extended and work Extended Stitching Table$")
    public void compareWorkExtendedAndStitchingWorkExt() {
        if (dataQualityStitchContext.recordsFromWorkExtended.isEmpty()) {
            Log.info("No Data Found ....");
        } else {
            for (int i = 0; i < dataQualityStitchContext.recordsFromWorkExtended.size(); i++) {
                String workId = dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id();
                getWorkExtendedJSONRec(workId);
                getWorkType(workId);
                Log.info("Work_Extended -> EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_JSON -> EPR => " + dataQualityStitchContext.recordsFromWorkStitching.getId());
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() != null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getId() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() + " is missing/not found in Work_Stitching table",
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            dataQualityStitchContext.recordsFromWorkStitching.getId());
                }

                Log.info(" EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> work_type => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type() +
                        " Work_JSON -> Type => " + dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype());
                if (dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type() != null ||
                        (dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype() != null)) {
                    Assert.assertEquals("The EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() + " is missing/not found in Work_Stitching table",
                            dataQualityStitchContext.recordsFromWorkExtended.get(i).getwork_type(),
                            dataQualityStitchContext.recFromWorkTypeStitchExtended.get(i).gettype());
                }
                String journalElsComInd_workExtTable;
                if(dataQualityStitchContext.recordsFromWorkExtended.get(i).getjournal_els_com_ind()){
                    journalElsComInd_workExtTable = "true";
                }else{
                    journalElsComInd_workExtTable = "false";
                }
                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalElsComInd => " + journalElsComInd_workExtTable +
                        " Work_JSON -> journalElsComInd => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());

                if (journalElsComInd_workExtTable!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd() != null)) {
                    Assert.assertEquals("The journalElsComInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            journalElsComInd_workExtTable,
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());
                }

                Log.info("EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id() +
                        " Work_Extended -> journalElsComInd => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getjourns_aims_scope() +
                        " Work_JSON -> journalElsComInd => " + dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalAimsScope());

                if (journalElsComInd_workExtTable!= null ||
                        (dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd() != null)) {
                    Assert.assertEquals("The journalElsComInd is incorrect for EPR => " + dataQualityStitchContext.recordsFromWorkExtended.get(i).getepr_id(),
                            journalElsComInd_workExtTable,
                            dataQualityStitchContext.recordsFromWorkStitching.getWorkExtended().getjournalElsComInd());
                }


            }

        }

    }


}