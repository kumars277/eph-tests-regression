package runner;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
@RunWith(Cucumber.class)
@CucumberOptions(

        glue = {"com.eph.automation.testing.steps", "com.eph.automation.testing.common.hooks"},
        features = "src/main/resources/features",
        plugin = {"pretty", "json:target/cucumber-reports/cucumber.json",
                           "junit:target/cucumber-reports/Cucumber.xml",
                            "html:target/cucumber-reports/feature-overview"},
        monochrome = true,
        tags = {"@PFRegressionSuite"}
        )

public class RunBDDTest {
    public RunBDDTest() {
    }
}
