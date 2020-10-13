//created by Nishant @ 29 Sep 2020
package com.eph.automation.testing.web.steps.BCSDataLakeAccess;
import com.eph.automation.testing.configuration.Constants;
import com.eph.automation.testing.configuration.DBManager;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.BCSDataQualityContext;
import com.eph.automation.testing.services.db.BCSDataLakeSQL.BCSDataLakeDataCheckSQL;
import com.eph.automation.testing.models.dao.BCSDataLake.*;
import com.google.common.base.Joiner;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BCSDataCheckSteps {

    private static String sql;
    private static List<String> Ids;
    private static BCSDataQualityContext bcsDataQualityContext;

    @Given("^We get the (.*) random ids from initial ingest (.*)$")
    public void getRandomIdsFromInitialIngest(String countOfRandomIds, String targetTable)
    {Log.info("getting random reference ids from initial ingest...");
        sql = String.format(BCSDataLakeDataCheckSQL.randomId_ingestTable, countOfRandomIds);
        List<Map<?, ?>> randomEPRIds = DBManager.getDBResultMap(sql, Constants.AWS_URL);
        Ids = randomEPRIds.stream().map(m -> (String) m.get("sourceref")).collect(Collectors.toList());
        Log.info("Randomly picked ids..."+ Ids);
    }

    @When("Get the data records from initial ingest")
    public void getInitialIngestData(){
        Log.info("We get initial ingest records...");
        sql = String.format(BCSDataLakeDataCheckSQL.getInitialIngestData, Joiner.on("','").join(Ids));
      //  Log.info(sql);
        bcsDataQualityContext.bcsInitialIngestDataObjectList = DBManager.getDBResultAsBeanList(sql, BCSInitialIngestDataObject .class, Constants.AWS_URL);
    }

}
