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

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"C# Timer trigger function executed at: {DateTime.Now}");
    readFileCtrl(log);
}

public static void readFileCtrl(TraceWriter log)
{
    using (SqlConnection conn = new SqlConnection())
    {
        SqlCommand sqlCommand;
        DataTable packageData = new DataTable();
        conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;

        var sqlText = "[DAF].[usp_GetRunGroupsToBeExecuted]";
        conn.Open();

        sqlCommand = new SqlCommand(sqlText, conn);
        sqlCommand.CommandType = CommandType.StoredProcedure;
        var sqlReader = sqlCommand.ExecuteReader();
        DataTable dtFileCtrl = new DataTable();
        dtFileCtrl.Load(sqlReader);
        var lstFilectrl = dtFileCtrl.AsEnumerable().Select(r => 
                            new FileCtrl {
                                FilesGroupID =  Convert.ToInt32( r["FilesGroupid"]),
                                FileName = r["Name"].ToString(),
                                RunGroupID = Convert.ToInt32(r["RunGroupid"]),
                                IsGrouped = r["IsGrouped"].ToString(),
                            }).ToList();

        string output = "";
        log.Info($"adding it to Queue");
        foreach (FileCtrl ctrl in lstFilectrl)
        {
            output = JsonConvert.SerializeObject(ctrl);
            addMsgQueue(output);
            updateFileStatus(ctrl.FilesGroupID);
        }
    }
}

public static void addMsgQueue(string msg)
{
    try
    {
        CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials("eafffsdevlanding", "kGyxzzT0dAw4Y4eJDK/0nZRGUBz+Z0i0HYcBDU2O/RzAFI3naYiq4jEeOqGJA0U9a5gswoREQoOttzqRbtmEWQ=="), true);
        CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
        var queue = queueClient.GetQueueReference("daflistner");
        var message = new CloudQueueMessage(msg);
        queue.AddMessage(message);
    }
    catch (Exception)
    {

        throw;
    }
}

private static void updateFileStatus(int filesGroupID)
{
    using (SqlConnection conn = new SqlConnection())
    {
        SqlCommand sqlCommand = new SqlCommand();
        conn.ConnectionString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
        conn.Open();
        sqlCommand.CommandText = "[DAF].[usp_UpdateFilesGroupStatus]"; //"Update DAF.FileCtrl SET FileStatus='AddedToQueue' WHERE FileID=" + fileID;
        sqlCommand.Connection = conn;
        sqlCommand.CommandType = CommandType.StoredProcedure;
        sqlCommand.Parameters.Add("@FilesGroupId", SqlDbType.Int).Value = filesGroupID;
        sqlCommand.Parameters.Add("@Status", SqlDbType.VarChar).Value = "AddedToQueue";

        sqlCommand.ExecuteNonQuery();
    }
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