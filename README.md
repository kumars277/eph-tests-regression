# EPH Automation Framework

## Synopsis
This is the Test Automation frame work to validate the End to End business functions for Enterprise Product Hub.
The full details of the test approach and validations - please refer confluence page  https://confluence.cbsels.com/display/arch/Product+Data+Management+-+3.2+Deployment+Workflow

## Installation
To install and run the project, run 
    mvn clean test -Dcucumber.options="--tags @Regression"
Make sure you have the correct version of maven \(3.3.9\), and  using Java 8.
