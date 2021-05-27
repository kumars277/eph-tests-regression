package com.eph.automation.testing.models.ui;

//created by Nishant @ 26 Apr 2021

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.eph.automation.testing.configuration.SecretsManagerHandler;
import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.mysql.cj.api.result.Row;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;
import net.minidev.json.JSONObject;


import javax.net.ssl.SSLHandshakeException;
import java.io.*;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class SpecificTasks {


    public boolean verifyFileExists(String filePath) {
        //created by Nishant @ 26 Apr 2021


       //     File temp;
        //      temp = File.createTempFile(filePath, fileType);
        //      boolean exists = temp.exists();

        boolean exists = new File(filePath).exists();
        System.out.println("file exists : " + exists);
        return exists;
    }


    public ArrayList<ArrayList<String>> readCsv(String csvfile) throws Exception
    {
        //created by Nishant @ 26 Apr 2021
        //updated by Nishant @ 07 May 2021
        ArrayList<String> csvRows = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        ArrayList<ArrayList<String>> RowList = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null)
        {
            csvRows.add(line.toString());
            ArrayList<String> columnList = customSplitSpecific(line.toString());
            //    System.out.println(columnList);
            RowList.add(columnList);

        }
        br.close();
        return RowList;
    }



    public void readS3File() throws IOException {
//created by Nishant @ 6 May 2021
        Regions clientRegion = Regions.fromName("eu-west-1");//Regions.DEFAULT_REGION; //"eu-west-1"
        String bucketName = "com-elsevier-jrbi-nonprod/sit/staging";

       // String bucketName = "https://com-elsevier-jrbi-nonprod.s3-eu-west-1.amazonaws.com/sit/staging";
        String key = "ops-10001-journal-metadata-eph-sit";

        BasicAWSCredentials awsCreds = new BasicAWSCredentials("accessKey", "secretKey");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(clientRegion).build();

        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));

        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));

        String s = null;
        while ((s = reader.readLine()) != null)
        {
            System.out.println(s);
            //your business logic here
        }

        /*
        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        InputStream objectData = object.getObjectContent();
// Process the objectData stream.
        objectData.close();
*/

    }

public ArrayList<ArrayList<String>> readS3fileAPI(String bucketName,String key) throws IOException {
    //created by Nishant @ 7 May 2021
    Regions clientRegion = Regions.fromName("eu-west-1");//Regions.DEFAULT_REGION; //"eu-west-1"
   // String bucketName = "com-elsevier-jrbi-nonprod/sit/staging";
   //  String key = "ops-10001-journal-metadata-eph-sit";

    ArrayList<ArrayList<String>> RowColumnData = new ArrayList<>();
    S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;

    //get credential from secret manager to access s3 bucket
    JSONObject object =  SecretsManagerHandler.getSecretKeyObj("eu-west-1","eph_s3bucket_access");

    try {

            BasicAWSCredentials creds = new BasicAWSCredentials(object.getAsString("aws_access_key_id"), object.getAsString("aws_secret_access_key"));
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new  AWSStaticCredentialsProvider(creds))
     //               .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Get an object and print its contents.
            System.out.println("Downloading s3 file");
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
          //  System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());

            RowColumnData = displayTextInputStream(fullObject.getObjectContent());

/*
            // Get a range of bytes from an object and print the bytes.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());

            // Get an entire object, overriding the specified response headers, and print the object's content.
            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            displayTextInputStream(headerOverrideObject.getObjectContent());
            */
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }

        return RowColumnData;

}


public void creatS3Bucket(String bucketName)
{//created by Nishant @ 10 May 2021
/*
    Region region = Region.US_WEST_2;
    S3Client s3 = S3Client.builder().region(region).build();
    String bucket = "new-bucket12345";

    CreateBucketRequest createBucketRequest = CreateBucketRequest
            .builder()
            .bucket(bucket)
            .createBucketConfiguration(CreateBucketConfiguration.builder()
                    .locationConstraint(region.id())
                    .build())
            .build();

    s3.createBucket(createBucketRequest);
    */

}

public void uploadToS3(String bucketName, String filetoUpload, String fileObjKeyName) throws IOException {
        //created by Nishant @ 10 May 2021

    Regions clientRegion = Regions.fromName("eu-west-1");

    String stringObjKeyName = "testobj";

    try {
        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();

        // Upload a text string as a new object.
        s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(filetoUpload));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.addUserMetadata("title", "someTitle");
        request.setMetadata(metadata);
        s3Client.putObject(request);
    } catch (AmazonServiceException e) {
        // The call was transmitted successfully, but Amazon S3 couldn't process
        // it, so it returned an error response.
        e.printStackTrace();
    } catch (SdkClientException e) {
        // Amazon S3 couldn't be contacted for a response, or the client
        // couldn't parse the response from Amazon S3.
        e.printStackTrace();
    }

}


    private static ArrayList<ArrayList<String>> displayTextInputStream(InputStream input) throws IOException {
        //created by Nishant @ 6 May 2021
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        ArrayList<ArrayList<String>> RowList = new ArrayList<>();

        while ((line = reader.readLine()) != null) {
          //  System.out.println(line);
            ArrayList<String> columnList = customSplitSpecific(line.toString());
        //    System.out.println(columnList);
            RowList.add(columnList);

        }

        return RowList;
    }


    public static ArrayList<String> customSplitSpecific(String s)
    {
        //created by Nishant @ 7 May 2021
        ArrayList<String> words = new ArrayList<String>();
        boolean notInsideComma = true;
        int start =0, end=0;
        for(int i=0; i<s.length()-1; i++)
        {
            if(s.charAt(i)==',' && notInsideComma)
            {
                words.add(s.substring(start,i));
                start = i+1;
            }
            else if(s.charAt(i)=='"')
                notInsideComma=!notInsideComma;
        }
        words.add(s.substring(start));
        return words;
    }

    public void writeCsv(String csvfile, List<String> Data)    {
        //created by Nishant @ 26 Apr 2021
        //if(!new File(csvfile).exists()) {
            try {
                FileWriter fileWriter = new FileWriter(csvfile,true);
                for(int i=0;i<Data.size();i++)
                {
                    fileWriter.append(Data.get(i));
                    fileWriter.append(",");
                }
                fileWriter.append("\n");
                fileWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        //}



    }


    public void writecsvHeader(String fileName) throws Exception
    {
        String[] Header = new String[4];
        Header[0]="RowNumber";
        Header[1]="link";
        Header[2]="status";
        Header[3]="comments";

        //  File file = new File (fileName);
        //if(!file.exists())writeCsv(fileName,Header);


        FileWriter fileWriter = new FileWriter(fileName,true);
        for(int i=0;i<Header.length;i++)
        {
            fileWriter.append(Header[i]);
            fileWriter.append(",");
        }
        fileWriter.append("\n");
        fileWriter.close();
    }

    public void resetValues(ArrayList<String> DatatoWrite)
    {
        DatatoWrite.clear();
    }


    public void verifySingleLink(String url) throws IOException {//created by Nishant @ 26 Apr 2020

        HttpURLConnection huc;
        int respCode = 0;
        boolean brokenLink;
        String statusDescription = "";
        String comment = "";

        if(url == null || url.isEmpty()){
            System.out.println("URL is either not configured for anchor tag or it is empty");
        }
else {

            try {
                huc = (HttpURLConnection) (new URL(url).openConnection());
                //    String cookie = huc.getHeaderField( "Set-Cookie").split(";")[0];
                //    huc.setRequestProperty("Accept","*/*");
                huc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0");
                //    huc.setRequestProperty("Cookie", cookie );
                huc.setRequestMethod("GET");
                huc.connect();
                respCode = huc.getResponseCode();

                System.out.println("status code :" + respCode);
                switch (respCode) {
                    case 200:
                        statusDescription = "valid link";
                        break;
                    case 301:
                        System.out.println("moved permanently to " + huc.getHeaderField("Location"));
                        statusDescription = "moved permanently";
                        comment = huc.getHeaderField("Location");
                        break;
                    case 401:
                        System.out.println("401 Unauthorized");
                        statusDescription = "401 Unauthorized";
                        break;
                    case 403:
                        System.out.println("403 Forbidden - The server understood the request but is refusing to fulfill it");
                        statusDescription = "403 Forbidden";
                    case 404:
                        System.out.println("page not found");
                        statusDescription = "Page not Found";
                        break;
                    //default:	System.out.println("less frequent error code: "+respCode);statusDescription="less frequent error code: "+respCode;break;
                }

                if (respCode >= 400) {
                    brokenLink = true;//EPR-W-102TM0
                    //     System.out.println("-------------------------------------->");
                    //     System.out.print("its a broken link");
                }
                //Verify.verify(!brokenLink,"found broken link \n"+url+"\n status code: "+respCode);

            } catch (MalformedURLException e) {
                System.out.println("not a valid url format");
                statusDescription = "not a valid url format";
            } catch (SSLHandshakeException e) {
                System.out.println(e.getMessage());
                statusDescription = e.getMessage();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                statusDescription = e.getMessage();
            } catch (ClassCastException e) {
                System.out.println(e.getMessage());
                System.out.println("unable to establish HttpURLConnection");
                statusDescription = "unable to establish HttpURLConnection";
            } finally {
                DataQualityContext.DataToWrite.add(String.valueOf(respCode)); //3rd column in result file
                DataQualityContext.DataToWrite.add(statusDescription); //4th column in result file
            }
        }
    }


    public void verifyLinksFromCsv(String csvFilePath, String resultCsvPath) throws Exception {
        //created by Nishant @ 26 Apr 2021
        String line = "";
        String splitBy = ",";

        try {
            //parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader(csvFilePath));
            int rowCnt = 0;
/*
            List<String> fileHeaders = new ArrayList<String>();
            fileHeaders.add(new String {"Row count", "link","status","comments"});
            writeCsv(resultCsvPath, fileHeaders);*/

            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                rowCnt += 1;
                System.out.println("");
                Log.info("...Row count - " + rowCnt);
                String[] testData = line.split(splitBy);
                System.out.println(testData[0]);
             //   List<String> linkStatus = verifySingleLink(testData[0]);

                List<String> data = new ArrayList<String>();
                writeCsv(resultCsvPath, data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCsv_method1(String resultCsvPath, List<String[]> Data) throws IOException {
//created by Nishant @ 26 Apr 2021
        createDir(resultCsvPath);
        CSVWriter writer = new CSVWriter(new FileWriter(resultCsvPath+"\\output2.csv",true));

        writer.writeAll(Data);
        writer.close();
    }

    public void createDir(String dir) {//created by Nishant @ 26 Apr 2021
        File file = new File(dir);
        boolean dirCreated = file.mkdir();
    }

    public void updateCSV(String fileToUpdate, int row, int col, String replace) throws Exception
    {//created by Nishant @ 26 Apr 2021

        File inputFile = new File(fileToUpdate);

        // Read existing file
        CSVReader reader = new CSVReader(new FileReader(inputFile));
        List<String[]> csvBody = reader.readAll();
        // get CSV row column  and replace with by using row and column
        csvBody.get(row)[col] = replace;
        reader.close();

        // Write to CSV file which is open
        CSVWriter writer = new CSVWriter(new FileWriter(inputFile));
        writer.writeAll(csvBody);
        writer.flush();
        writer.close();
    }

public String getDateNTime()
{//created by Nishant @ 3 May 2021

    Calendar currentDate = Calendar.getInstance();
    SimpleDateFormat formatter=  new SimpleDateFormat("yyyy_MMM_dd HH_mm_ss");
    String datewithtime = formatter.format(currentDate.getTime());
    String datenow = datewithtime.split(" ")[0].replace("/", "_");
    return datewithtime;

}

}
