# EPH Automation Framework

## Synopsis
This is the Test Automation frame work to validate the End to End business functions for Enterprise Product Hub.
The full details of the test approach and validations - please refer confluence page  https://confluence.cbsels.com/display/arch/Product+Data+Management+-+3.2+Deployment+Workflow

## Installation
To install and run the project, run 
    mvn clean test -Dcucumber.options="--tags @Regression"
Make sure you have the correct version of maven \(3.3.9\), and  using Java 8.


--API known issue
1.  when we search by title it cause mismatch with DB sometimes.
mismatch (title=Care) is caused by EPR-W-11N3YY.
The title is 'Poincaré-Andronov-Melnikov Analysis for Non-Smooth Systems'
and ElasticSearch ignores accents so considers caré a match for care.
There doesn't appear to be an easy way to do an accent insensitive search in Postgres
without installing an extension
but you just use where (work_title ~*'Care' or work_title ~*'Caré')
since 'care' is a random search keyword hence no fix is applied to API script yet.
hence similar mismatch may occur by script.