package features.zOnHold.znotused.batch;

/**
 * Created by RAVIVARMANS on 11/26/2018.
 */
public class JobConfiguration {

    public String Run_Batch;
    public String Run_FTP;
    public String submit_ref;
    public String Jobs_Done;
    public String batchId;
    public String loadId;


    public void setRun_Batch(String run_Batch) {
        Run_Batch = run_Batch;
    }

    public void setRun_FTP(String run_FTP) {
        Run_FTP = run_FTP;
    }

    public void setSubmit_ref(String submit_ref) {
        this.submit_ref = submit_ref;
    }

    public void setJobs_Done(String jobs_Done) {
        Jobs_Done = jobs_Done;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public void setLoadId(String loadId) {
        this.loadId = loadId;
    }
}
