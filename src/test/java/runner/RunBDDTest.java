package runner;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        strict = false,
        features = "src/main/resources/features",
        plugin= {"pretty", "json:target/cucumber.json","html:target/cucumber-reports" },
        tags = { "@stitchingData" },
        glue = {"com.eph.automation.testing.web.steps", "com.eph.automation.testing.common.hooks"})

public class RunBDDTest {
    public RunBDDTest() {
    }
}
