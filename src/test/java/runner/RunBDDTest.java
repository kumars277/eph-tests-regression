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
        format= {"pretty","json:target/cucumber.json" },
        tags = {"@CkAPI"},
        glue = {"com.eph.automation.testing.steps", "com.eph.automation.testing.common.hooks"})

public class RunBDDTest {
    public RunBDDTest() {
    }
}
