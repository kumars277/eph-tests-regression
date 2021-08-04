package features.zOnHold.znotused.batch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by RAVIVARMANS on 7/30/2018.
 */
public class LimitedTimeCondition {
    private CountDownLatch conditionMetLatch;
    private Integer unitsCount;
    private TimeUnit unit;

    public LimitedTimeCondition(final Integer unitsCount, final TimeUnit unit)
    {
        conditionMetLatch = new CountDownLatch(75);
        this.unitsCount = unitsCount;
        this.unit = unit;
    }

    public boolean waitForConditionToBeMet()
    {
        try
        {
            while (conditionMetLatch.getCount() > 1 ) {
                boolean myBatchStatus = JobStatusService.getJobStatusByBatchID();
                if (!myBatchStatus) {
                    System.out.println("Polling Count - Remaining times >>>"+conditionMetLatch.getCount() + " Batch Status "+myBatchStatus);
                    conditionMetLatch.await(unitsCount, unit);
                    conditionWasMet();
                } else {
                    return true;
                }
            }
            return false;
        }
        catch (final InterruptedException e)
        {
            System.out.println("Someone has disturbed the condition awaiter.");
            return false;
        }

    }

    public void conditionWasMet()
    {
        conditionMetLatch.countDown();
    }
}
