package com.eph.automation.testing.models;


import com.eph.automation.testing.annotations.StaticInjection;

/**
 * Created by RAVIVARMANS on 11/24/2018.
 */
@StaticInjection
public class TestProperties {
    public String browserType;
    public Boolean gridRun;
    public String environment;
    public Boolean targetDB;
    public String s3Key;
    public int rowFrom;
    public int rowTill;
}
