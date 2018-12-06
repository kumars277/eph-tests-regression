package database;

import com.eph.automation.testing.annotations.StaticInjection;
import com.eph.automation.testing.models.contexts.LoadBatchContext;
import com.eph.automation.testing.services.db.DataLoadServiceImpl;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by RAVIVARMANS on 29/11/2018.
 */
public class MDMIntegrationTest {

    @StaticInjection
    public LoadBatchContext loadBatchContext;

    @Test
    @Ignore
    public void testPostgresStoredFunctionToGetLoadID() {
        DataLoadServiceImpl.createProductByStoreProcedure();
        System.out.println(loadBatchContext.batchId);
    }
}
