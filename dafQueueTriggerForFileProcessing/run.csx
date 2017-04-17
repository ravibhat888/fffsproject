#r "System.Runtime"
#r "System.Threading.Tasks"
#r "System.Data"
#r "Microsoft.WindowsAzure.Storage"
#r "System.Linq"
#r "System.Data.DataSetExtensions"


using System;
using System.Net;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using Microsoft.Azure.Common;
using System.Configuration;
using System.Data.SqlClient;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Auth;
using Newtonsoft.Json;
using System.Data.DataSetExtensions;
using Microsoft.Azure.Management.HDInsight.Job;
using Microsoft.Azure.Management.HDInsight.Job.Models;
using System.Text;
using System.Threading;
using Hyak.Common;

public static void Run(string myQueueItem, TraceWriter log)
{
    log.Info($"C# Queue trigger function processed: {myQueueItem}");

    FileCtrl fileCtrl = JsonConvert.DeserializeObject<FileCtrl>(myQueueItem);


    log.Info($"File ID: {fileCtrl.FilesGroupID}");
    log.Info($"File Name: {fileCtrl.FileName}");
    log.Info($"RUn group: {fileCtrl.RunGroupID}");
   
    ExecutePackage objPackageExec = new ExecutePackage();
    //objPackageExec.readFileCtrl();
    //objPackageExec.addMsgQueue();
    objPackageExec.executePipleline(log,fileCtrl.FilesGroupID,fileCtrl.RunGroupID,fileCtrl.FileName, fileCtrl.IsGrouped);

     log.Info($"C# Queue trigger finishied execution");
}

public class ExecutePackage 
{
    private int ctrlPackageID, servicePkgID, noofRecordsAff, generatedRecord, filesGroupID, runGroupID;
    public string strReserviorConString, strTimeStamp, fileName, isGrouped;
    private Dictionary<string, string> scalaConfigList;
    private HDInsightJobManagementClient _hdiJobManagementClient;
    //public string cnnString;
    private string exceptionMsg;
    public void executePipleline(TraceWriter log, int fID, int runGrpID, string fName, string isGroupd)
    {
        string packageName;
        List<string> scalaArgs = null;
        int packageID, packageRunGroupID;
        string  cleanUpSPName, exitRefCode="", dataEntityRefCode;
        bool errorThrown=false;
        runGroupID = runGrpID;
        filesGroupID = fID;
        fileName = fName;
        isGrouped = isGroupd;
        log.Info($"Called executePipleline");
        //cnnString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
        getResConnectionString();
        scalaConfigList = getScalaConfigs();
        cleanUpSPName = getCleanUpProcName(runGroupID);
        DataTable packageData = getPackageDetails(runGroupID);

        using (packageData)
        {
            executeCleanupSP(cleanUpSPName, runGroupID, "START");
            executeCtrlPkgStoredProcedure(PackageType.Controlling, "Insert", runGroupID);
            strTimeStamp = DateTime.Now.ToString("yyyyMMddhhmmss") + "-" + ctrlPackageID;
            updateGenFileName();
            foreach (DataRow row in packageData.Rows)
            {
                packageName = row["PackageName"].ToString();
                packageID = Convert.ToInt32(row["packageID"].ToString());
                packageRunGroupID = Convert.ToInt32(row["packageRunGroupID"].ToString());
                exitRefCode = row["ExitActionRefCode"].ToString();
                dataEntityRefCode = row["DataEntityRefCode"].ToString();
                exceptionMsg = "";
                executeSrvcPkgStoredProcedure(PackageType.Service, "Insert", runGroupID, packageID);
                if (row["implementationtype"].ToString().ToLower().Equals(ImplementationType.SCALA.ToString().ToLower()))
                {
                    log.Info($"Called Scala Job :" + packageName);
                    log.Info($"filename : { fileName }");
                    log.Info($"TimeStamp : { strTimeStamp }");
                    scalaArgs = getScalaParameters(packageID, runGroupID, fileName);
                    log.Info($"Scala Params -  {String.Join(String.Empty, scalaArgs.ToArray())}");
                    errorThrown = SubmitMRJob(packageName, scalaArgs,log);
                    if (errorThrown)
                    {
                        logError(exceptionMsg, servicePkgID, packageName, dataEntityRefCode);
                    }
                    log.Info($"Finished Scala Job :" + packageName);
                }
                else if (row["implementationtype"].ToString().ToLower().Equals(ImplementationType.STOREDPROCEDURE.ToString().ToLower()))
                {
                    log.Info($"Called SP :" + packageName);
                    errorThrown = invokeStoredProcedure(packageName);
                    if (errorThrown)
                    {
                        logError(exceptionMsg, servicePkgID, packageName, dataEntityRefCode);
                    }
                    log.Info($"Finished SP :" + packageName);
                }
                executeSrvcPkgStoredProcedure(PackageType.Service, (errorThrown ==true)? "FAIL":"Update", runGroupID, packageID);
                if (errorThrown == true && exitRefCode.Equals("ABORT")) {
                    log.Info($"Aborting execution");
                    break;
                }
            }
            if (errorThrown == true && exitRefCode.Equals("ABORT"))
            {
                executeCtrlPkgStoredProcedure(PackageType.Controlling, "FAIL", runGroupID);
                updateFileStatus(filesGroupID,"Failed");
            }
            else
            {
                executeCtrlPkgStoredProcedure(PackageType.Controlling, "Update", runGroupID);
                executeCleanupSP(cleanUpSPName, runGroupID, "END");
                updateFileStatus(filesGroupID,"Loaded");
            }
            log.Info($"Execution Completed.");
            
        }
    }

    //Retrieve Scala paramters from table for the respective package and rungroup ID.
    private List<string> getScalaParameters(int packageID, int runGroupID, string fName)
    {
        List<string> arguments = new List<string>();
        string args = string.Empty;

        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand;
            string sqlText = null;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString; //"Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevMetadata;User ID=sqladmin;Password=Pass01word";
            sqlText = "SELECT  ParameterDescription, parametervalue FROM daf.packagerungroup pkgGRP JOIN daf.parameter prm ON pkgGRP.packagerungroupID = prm.packagerungroupID " +
                        "WHERE rungroupid = " + runGroupID +" AND packageid= " + packageID + " ORDER BY pkgGRP.LastModifiedDate Desc";
            conn.Open();

            sqlCommand = new SqlCommand(sqlText, conn);
            sqlCommand.CommandType = CommandType.Text;
            //command.Parameters.Add(new SqlParameter("@FileName", "abc34.txt"));
            using (var reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    args="";
                    args = reader["ParameterDescription"].ToString().Trim();
                    arguments.Add(args);
                    if (args.Equals("--app_arguments"))
                    {
                        args = " " + reader["parametervalue"].ToString() + " " + fName + " " + strTimeStamp + ((isGrouped == "Y")? " " + scalaConfigList["SCALA_DAF_METADB"]: "");
                    }
                    else
                        args = " " + reader["parametervalue"].ToString();

                    arguments.Add(args);


                }
            }
        }
        return arguments; //args.Split(',').ToList(); 
    }

    private bool invokeStoredProcedure(string spName)
    {
        try
        {
            using (SqlConnection conn = new SqlConnection())
            {
                SqlCommand sqlCommand;
                SqlParameter sqlOutParam = null;
                SqlParameter sqlOutParam1 = null;
                string sql = null;
                //conn.ConnectionString = "Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevReservoir;User ID=sqladmin;Password=Pass01word";
                conn.ConnectionString = strReserviorConString;
                sql = spName;
                conn.Open();

                sqlCommand = new SqlCommand(sql, conn);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                sqlCommand.CommandTimeout = 600;
                sqlCommand.Parameters.Add("@ControllingPackageRunid", SqlDbType.Int).Value = ctrlPackageID;
                sqlOutParam = new SqlParameter("@ProcessedCount", SqlDbType.Int);
                sqlOutParam.Direction = ParameterDirection.Output;
                sqlCommand.Parameters.Add(sqlOutParam);
                sqlOutParam1 = new SqlParameter("@GeneratedCount", SqlDbType.Int);
                sqlOutParam1.Direction = ParameterDirection.Output;
                sqlCommand.Parameters.Add(sqlOutParam1);
                sqlCommand.ExecuteNonQuery();

                if (!(sqlOutParam == null))
                    noofRecordsAff = Convert.ToInt32(sqlOutParam.Value.ToString());
                if (!(sqlOutParam1 == null))
                    generatedRecord = Convert.ToInt32(sqlOutParam1.Value.ToString());

            }
        }
        catch (SqlException ex)
        {
            exceptionMsg = ex.Message + "/r/n" + ex.InnerException;
            return true;
        }
        return false;
    }

    private void executeCtrlPkgStoredProcedure(PackageType pkgType, string insertType, int runGroupID)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlOutParam = null;
            string sql = string.Empty;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;//"Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevMetadata;User ID=sqladmin;Password=Pass01word";
            if (PackageType.Controlling.ToString() == pkgType.ToString() && insertType.Equals("Insert"))
            {
                    
                sql = "[DAF].[usp_InsertControllingPackageRun]";
                sqlCommand.Parameters.Add("@ControllingPackageName", SqlDbType.VarChar).Value = "Azure function";
                sqlCommand.Parameters.Add("@RunBy", SqlDbType.VarChar).Value = "System User";
                sqlCommand.Parameters.Add("@ProgressStatusRefCode", SqlDbType.VarChar).Value = "InProgress";
                sqlCommand.Parameters.Add("@RunGroupID", SqlDbType.Int).Value = runGroupID;
                sqlCommand.Parameters.Add("@FilesGroupID", SqlDbType.Int).Value = filesGroupID;
                sqlOutParam = new SqlParameter("@ControllingPackageRunID", SqlDbType.Int);
                sqlOutParam.Direction = ParameterDirection.Output;
                sqlCommand.Parameters.Add(sqlOutParam);
                sqlCommand.CommandText = sql;
            }
            else if (PackageType.Controlling.ToString() == pkgType.ToString() && (insertType.Equals("Update") || insertType.Equals("FAIL")))
            {
                sql = "[DAF].[usp_UpdateControllingPackageRun]";
                sqlCommand.Parameters.Add("@ControllingPackageRunID", SqlDbType.Int).Value = ctrlPackageID;
                sqlCommand.Parameters.Add("@ProgressStatusRefCode", SqlDbType.VarChar).Value = insertType.Equals("Update") ? "Completed" : "FAILED";;
                sqlCommand.CommandText = sql;
            }
            conn.Open();

            sqlCommand.Connection = conn;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 600;
            sqlCommand.ExecuteNonQuery();

            if(!(sqlOutParam == null))
                ctrlPackageID = Convert.ToInt32(sqlOutParam.Value.ToString());
        }
    }

    private void executeSrvcPkgStoredProcedure(PackageType pkgType, string insertType, int runGroupID, int packageID)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlOutParam = null;
            string sql = string.Empty;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;//"Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevMetadata;User ID=sqladmin;Password=Pass01word";
            if (PackageType.Service.ToString() == pkgType.ToString() && insertType.Equals("Insert"))
            {
                sql = "[DAF].[usp_InsertServicePackageRun]";
                sqlCommand.Parameters.Add("@ControllingPackageRunID", SqlDbType.Int).Value = ctrlPackageID;
                sqlCommand.Parameters.Add("@Packageid", SqlDbType.Int).Value = packageID;
                sqlCommand.Parameters.Add("@ProgressStatusRefCode", SqlDbType.VarChar).Value = "InProgress";
                sqlOutParam = new SqlParameter("@ServicePackageRunId", SqlDbType.Int);
                sqlOutParam.Direction = ParameterDirection.Output;
                sqlCommand.Parameters.Add(sqlOutParam);
                sqlCommand.CommandText = sql;
            }
            else if (PackageType.Service.ToString() == pkgType.ToString() && (insertType.Equals("Update") || insertType.Equals("FAIL")))
            {
                sql = "[DAF].[usp_UpdateServicePackageRun]";
                sqlCommand.Parameters.Add("@ServicePackageRunId", SqlDbType.Int).Value = servicePkgID;
                sqlCommand.Parameters.Add("@ProgressStatusRefCode", SqlDbType.VarChar).Value = insertType.Equals("Update")? "Completed": "FAILED";
                sqlCommand.Parameters.Add("@NumRecordsProcessed", SqlDbType.Int).Value = noofRecordsAff;
                sqlCommand.Parameters.Add("@NumRecordsGenerated", SqlDbType.Int).Value = generatedRecord;
                sqlCommand.CommandText = sql;
            }
            conn.Open();

            sqlCommand.Connection =  conn;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 600;
            sqlCommand.ExecuteNonQuery();

            if (!(sqlOutParam == null))
                servicePkgID = Convert.ToInt32(sqlOutParam.Value.ToString());

        }
    }

    private string getResConnectionString()
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand;
            string sql = null;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;//"Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevMetaData;User ID=sqladmin;Password=Pass01word";
            sql = "SELECT TOP 1 * FROM DAF.Configuration WHERE ConfigGroupid= 1 AND ConfigKey = 'RESERVOIR_DB'";
            conn.Open();
            sqlCommand = new SqlCommand(sql, conn);
            sqlCommand.CommandType = CommandType.Text;
            using (var reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    strReserviorConString = reader["ConfigValue"].ToString().Trim();
                }
            }
        }
        return strReserviorConString;
    }

    private Dictionary<string,string>  getScalaConfigs()
    {
        Dictionary<string,string> scalaConfigs = new Dictionary<string, string>();
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand;
            string sql = null;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;//"Data Source=eafffsdevelopment.database.windows.net,1433;Initial Catalog=EAFFFSDevMetaData;User ID=sqladmin;Password=Pass01word";
            sql = "SELECT ConfigKey,ConfigValue FROM DAF.Configuration c JOIN DAF.Configurationgroup cg ON cG.ConfigGroupID= c.ConfigGroupID WHERE CG.Shortname= 'Scala'";
            conn.Open();
            sqlCommand = new SqlCommand(sql, conn);
            sqlCommand.CommandType = CommandType.Text;
            using (var reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    scalaConfigs.Add(reader["ConfigKey"].ToString().Trim(), reader["ConfigValue"].ToString().Trim());
                }
            }
        }
        return scalaConfigs;
    }

    private bool SubmitMRJob(string packageName, List<string> args, TraceWriter log)
    {
        var clusterCredentials = new BasicAuthenticationCloudCredentials { Username = scalaConfigList["SCALA_USERNAME"], Password = scalaConfigList["SCALA_PASSWORD"] };
        _hdiJobManagementClient = new HDInsightJobManagementClient(scalaConfigList["SCALA_NAME"] + scalaConfigList["SCALA_URI"], clusterCredentials);

        List<string> arg = new List<string>();
        //arg.Add(args.Replace('\"', '"'));
        var paras = new MapReduceJobSubmissionParameters
        {
            //JarFile = @"/home/ssh-admin/com.adf.sparklauncher.jar",
            JarFile = "wasb://sparkjars@eafffsdevstaging.blob.core.windows.net/com.adf.sparklauncher.jar",
            JarClass = "com.adf.sparklauncher.AdfSparkJobLauncher",
            Arguments = args
        };

        log.Info($"Submitting the MR job to the cluster...");
        var jobResponse = _hdiJobManagementClient.JobManagement.SubmitMapReduceJob(paras);
        var jobId = jobResponse.JobSubmissionJsonResponse.Id;
        log.Info($"Response status code is " + jobResponse.StatusCode);
        log.Info($"JobId is " + jobId);

        log.Info($"Waiting for the job completion ...");

        // Wait for job completion
        var jobDetail = _hdiJobManagementClient.JobManagement.GetJob(jobId).JobDetail;
        while (!jobDetail.Status.JobComplete)
        {
            Thread.Sleep(1000);
            jobDetail = _hdiJobManagementClient.JobManagement.GetJob(jobId).JobDetail;
        }

        // Get job output
        var storageAccess = new AzureStorageAccess(scalaConfigList["SCALA_ACCOUNTNAME"], scalaConfigList["SCALA_ACCOUNTKEY"],
            scalaConfigList["SCALA_CONTAINERNAME"]);
        var output = (jobDetail.ExitValue == 0)
            ? _hdiJobManagementClient.JobManagement.GetJobOutput(jobId, storageAccess) // fetch stdout output in case of success
            : _hdiJobManagementClient.JobManagement.GetJobErrorLogs(jobId, storageAccess); // fetch stderr output in case of failure

        log.Info($"Job output is: ");

        using (var reader = new StreamReader(output, Encoding.UTF8))
        {
            string value = reader.ReadToEnd();
            log.Info(value);
            if (!value.Contains("Spark Job succeeded"))
            {
                exceptionMsg = value;
                return true;
            }
            else
                return false;
        }
    }

    private string getCleanUpProcName(int runGroup)
    {
        string cleanupSPName = string.Empty;
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand;
            string sql = null;
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
            sql = "SELECT TOP 1 CleanupStoredProcedureName FROM DAF.[RunGroup] WHERE RunGroupID=" + runGroup;
            conn.Open();
            sqlCommand = new SqlCommand(sql, conn);
            sqlCommand.CommandType = CommandType.Text;
            using (var reader = sqlCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    cleanupSPName = reader["CleanupStoredProcedureName"].ToString().Trim();
                }
            }
        }
        return cleanupSPName;
    }

    private void executeCleanupSP(string SPName, int runGroupID, string action)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
            conn.Open();
            sqlCommand.Parameters.Add("@RunGroupID", SqlDbType.Int).Value = runGroupID;
            sqlCommand.Parameters.Add("@Action", SqlDbType.VarChar).Value = action;
            sqlCommand.CommandText = SPName;
            sqlCommand.Connection = conn;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 600;
            sqlCommand.ExecuteNonQuery();
        }
    }

    private void logError(string Error, int srvcPackageRunID, string packageName, string dataEntityRefCode)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
            conn.Open();
            sqlCommand.Parameters.Add("@SeverityLevelRefCode", SqlDbType.VarChar).Value = "ERROR";
            sqlCommand.Parameters.Add("@ServicePackageRunID", SqlDbType.Int).Value = srvcPackageRunID;
            sqlCommand.Parameters.Add("@EventSource", SqlDbType.VarChar).Value = packageName;
            sqlCommand.Parameters.Add("@EventText", SqlDbType.VarChar).Value = Error;
            sqlCommand.Parameters.Add("@ExceptionCode", SqlDbType.VarChar).Value = "";
            sqlCommand.Parameters.Add("@DataEntityRefCode", SqlDbType.VarChar).Value = dataEntityRefCode;
            sqlCommand.CommandText = "[DAF].[usp_InsertEventLog]";
            sqlCommand.Connection = conn;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 600;
            sqlCommand.ExecuteNonQuery();
            sqlCommand.Dispose();
        }
    }

    private void updateGenFileName()
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
            conn.Open();
            sqlCommand.Parameters.Add("@Stamp", SqlDbType.VarChar).Value = strTimeStamp;
            sqlCommand.Parameters.Add("@FilesGroupId", SqlDbType.Int).Value = filesGroupID;
            sqlCommand.CommandText = "[DAF].[usp_UpdateGeneratedFileName]";
            sqlCommand.Connection = conn;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandTimeout = 600;
            sqlCommand.ExecuteNonQuery();
            sqlCommand.Dispose();
        }
    }

    private DataTable getPackageDetails(int runGroupID)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand;
            DataTable packageData = new DataTable();
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;

            var sqlText = "[DAF].[usp_GetPackageExecutionList]"; //"SELECT * FROM DAF.vw_GetPackageExecutionData WHERE Rungroupid =" + runGroupID + " ORDER BY executionOrderNum ASC";
            conn.Open();

            sqlCommand = new SqlCommand(sqlText, conn);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.Parameters.Add("@RunGroupid", SqlDbType.Int).Value = runGroupID;
            var reader = sqlCommand.ExecuteReader();
            packageData.Load(reader);
            reader.Close();
            return packageData;
        }
    }
    private void updateFileStatus(int filesGroupID, string status)
    {
        using (SqlConnection conn = new SqlConnection())
        {
            SqlCommand sqlCommand = new SqlCommand();
            conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
            conn.Open();
            sqlCommand.CommandText = "[DAF].[usp_UpdateFilesGroupStatus]";

            //"Update DAF.FileCtrl SET FileStatus='Completed' WHERE FileID=" + fileID;
            sqlCommand.Connection = conn;
            sqlCommand.Parameters.Add("@FilesGroupId", SqlDbType.Int).Value = filesGroupID;
            sqlCommand.Parameters.Add("@Status", SqlDbType.VarChar).Value = status;
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.ExecuteNonQuery();
        }
    }
}


public enum ImplementationType
{
    STOREDPROCEDURE,
    SCALA,
    DOTNET
}

public enum PackageType
{
    Controlling,
    Service,
}

public class FileCtrl
{
    public int FilesGroupID { get; set; }
    public string FileName { get; set; }
    public int RunGroupID { get; set; }
    public int FileTypeID { get; set; }
    string      GeneratedFilePath { get; set; }
    public string      IsGrouped { get; set; }
}