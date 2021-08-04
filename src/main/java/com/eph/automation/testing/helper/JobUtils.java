package com.eph.automation.testing.helper;

import com.eph.automation.testing.annotations.StaticInjection;
import features.zOnHold.znotused.context.LoadBatchContext;
import features.zOnHold.znotused.batch.LimitedTimeCondition;

import java.util.concurrent.TimeUnit;

/**
 * Created by RAVIVARMANS on 30/11/2018.
 */
public class JobUtils {

    @StaticInjection
    public static LoadBatchContext loadBatchContext;

    public static void waitTillTheBatchComplete() {
        LimitedTimeCondition batchRunningStatus = new LimitedTimeCondition(5, TimeUnit.SECONDS);
        if (batchRunningStatus.waitForConditionToBeMet()) {
            System.out.println(loadBatchContext.jobStatus);
        }
    }

    public static void waitForTheJob(int mins) {
        try {
            Thread.sleep(mins * 60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
