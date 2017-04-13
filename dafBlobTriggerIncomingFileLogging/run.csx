#r "System.Runtime"
#r "System.Threading.Tasks"
#r "System.Data"
#r "Microsoft.WindowsAzure.Storage"
using System;
using System.Net;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Azure.Common;
using System.Configuration;
using System.Data.SqlClient;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

public static void Run(CloudBlockBlob myBlob, string name, TraceWriter log) //Stream
{
    log.Info($"C# Blob trigger");
    log.Info($"FilePath : " + myBlob.StorageUri);
    log.Info($"FilePath-P : " + myBlob.Uri);
    log.Info($"FilePath : " + name);
    configureIncomingFile(name);
    //log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
    
}

private static void configureIncomingFile(string fileName)
{
    int filesGroupID=0;
    using (SqlConnection conn = new SqlConnection())
    {
        SqlCommand sqlCommand;
        SqlParameter sqlOutParam = null;
        DataTable packageData = new DataTable();
        var cnnString = ConfigurationManager.ConnectionStrings["SqlConnection"].ConnectionString;
        conn.ConnectionString = cnnString;

        var sqlText = "DAF.usp_ConfigureIncomingFile";
        conn.Open();

        sqlCommand = new SqlCommand(sqlText, conn);
        sqlCommand.CommandType = CommandType.StoredProcedure;
        sqlCommand.CommandTimeout = 600;
        sqlCommand.Parameters.Add("@FileName", SqlDbType.VarChar).Value = fileName;
        //sqlCommand.Parameters.Add("@FilePath", SqlDbType.VarChar).Value = filePath;
        sqlOutParam = new SqlParameter("@FilesGroupId", SqlDbType.Int);
        sqlOutParam.Direction = ParameterDirection.Output;
        sqlCommand.Parameters.Add(sqlOutParam);

        sqlCommand.ExecuteNonQuery();

        if(!(sqlOutParam == null))
                filesGroupID = Convert.ToInt32(sqlOutParam.Value.ToString());

        sqlCommand = new SqlCommand("DAF.usp_MarkFileReadyToLoad", conn);
        sqlCommand.CommandType = CommandType.StoredProcedure;
        sqlCommand.CommandTimeout = 600;
        sqlCommand.Parameters.Add("@FilesGroupId", SqlDbType.VarChar).Value = filesGroupID;
        sqlCommand.ExecuteNonQuery();
    }
}