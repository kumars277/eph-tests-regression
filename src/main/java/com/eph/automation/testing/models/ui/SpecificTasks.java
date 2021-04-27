package com.eph.automation.testing.models.ui;

//created by Nishant @ 26 Apr 2021

import com.eph.automation.testing.helper.Log;
import com.eph.automation.testing.models.contexts.DataQualityContext;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import javax.net.ssl.SSLHandshakeException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class SpecificTasks {


    public boolean verifyFileExists(String filePath, String fileType) {
        //created by Nishant @ 26 Apr 2021
        File temp;
        try {
            temp = File.createTempFile(filePath, fileType);
            boolean exists = temp.exists();
            System.out.println("file exists : " + exists);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    public ArrayList<String> readCsv(String csvfile) throws Exception
    {
        //created by Nishant @ 26 Apr 2021
        ArrayList<String> csvRows = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new FileReader(csvfile));
        String line = null;
        while ((line = br.readLine()) != null) { csvRows.add(line.toString());  }
        br.close();
        return csvRows;
    }

    public void writeCsv(String csvfile, List<String> Data) throws Exception
    {//created by Nishant @ 26 Apr 2021
        if(!new File(csvfile).exists())  writecsvHeader(4,csvfile);

        FileWriter fileWriter = new FileWriter(csvfile,true);

        for(int i=0;i<Data.size();i++)
        {
            fileWriter.append(Data.get(i));
            fileWriter.append(",");
        }
        fileWriter.append("\n");
        fileWriter.close();
    }


    public void writecsvHeader(int size, String fileName) throws Exception
    {
        String[] Header = new String[size];
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
        int respCode;
        boolean brokenLink;
        String statusDescription = "";
        String comment = "";
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

            DataQualityContext.DataToWrite.add(String.valueOf(respCode)); //3rd column in result file


        } catch (MalformedURLException e) {
            System.out.println("not a valid url format");
            statusDescription = "not a valid url format";
        } catch (SSLHandshakeException e) {
            System.out.println(e.getMessage());
            statusDescription =e.getMessage();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            statusDescription =e.getMessage();
        } catch (ClassCastException e) {
            System.out.println(e.getMessage());
            System.out.println("unable to establish HttpURLConnection");
            statusDescription ="unable to establish HttpURLConnection";
        }
        finally {
            DataQualityContext.DataToWrite.add(statusDescription); //4th column in result file
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


}
