
using System;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Diagnostics;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"C# manually triggered function called");
    ExecuteBatch batch = new ExecuteBatch();
    batch.MainAsync(log).Wait();
}

class ExecuteBatch
{
    private string BatchAccountName; // = "fffsbatchtest";
    private string BatchAccountKey ; //= "yt5WxQ3RzdjvmIjx8sDwEI3CzngfEv3kHggJoQGCiv+NwrOynXmIEo2TFneQD6O4rgl9U6StTJYgf1dIT+WyIg==";
    private string BatchAccountUrl ; //= "https://fffsbatchtest.northeurope.batch.azure.com";
<<<<<<< HEAD
=======

>>>>>>> b28861c103dac2a8a48d1edfedc2d252dad1ae42
    // Storage account credentials
    private string StorageAccountName ; //= "fffsbatchstorage";
    private string StorageAccountKey ; //= "q+FYKZfaYINPNIuSPCwnhmS80M4MuMA2mL740q4HU2d3gh2+uENf491CBv4kh85V5czojrigVC7iRFsIJ2vf4A==";

    private string PoolId = ""; //"FFFSCatchmentAvgPoolTest1";
    private string JobId = "FFFSCatchmentAvgJob" + "_" + DateTime.Now.Ticks.ToString();
    private TraceWriter log;
    private List<string> inputContainerList = new List<string>();
    private string appContainerName = ""; //"batchapplication";
    //const string inputContainerName = "input";
    private string outputContainerName = ""; // "telemetry";
    private string StagingAccName= "";
     private string StagingAccKey= "";
    private string stagingOutputName = "";
<<<<<<< HEAD
    
=======
>>>>>>> b28861c103dac2a8a48d1edfedc2d252dad1ae42
    public async Task MainAsync(TraceWriter _log)
    {
        log = _log;
        log.Info($"Sample start: {DateTime.Now.ToString()}" );
      
        Stopwatch timer = new Stopwatch();
        timer.Start();
        inputContainerList.AddRange( new string[] { "northeast-h13", "northeast-h19", "northeast-m2", "northeast-n6", "northeast-n7", "northeast-n9", "northeast-n10" });
       
       //inputContainerList.AddRange( new string[] { "northeast-n7" });
       
         LoadEnvironmentVariables();
        // Construct the Storage account connection string
        string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                        StorageAccountName, StorageAccountKey);
        log.Info($"ac name {storageConnectionString}");
        // Retrieve the storage account
        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

        // Create the blob client, for use in obtaining references to blob storage containers
        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

        // Upload the application and its dependencies to Azure Storage. This is the application that will
        // process the data files, and will be executed by each of the tasks on the compute nodes.
        List<ResourceFile> applicationFiles = GetListOfBlobResources(blobClient, appContainerName);

       // Obtain a shared access signature that provides write access to the output container to which
        // the tasks will upload their output.
        string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);
        log.Info($"Container URL : {outputContainerSasUrl}");
        // Create a BatchClient. We'll now be interacting with the Batch service in addition to Storage
        BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

        log.Info($"Before batch creation :");
        
        using (BatchClient batchClient = BatchClient.Open(cred))
        {
            try
            {
                log.Info($"Batch creation");
                log.Info($"Pool ID : {PoolId}");
                // Create the pool that will contain the compute nodes that will execute the tasks.
                // The ResourceFile collection that we pass in is used for configuring the pool's StartTask
                // which is executed each time a node first joins the pool (or is rebooted or reimaged).

                await CreatePoolIfNotExistAsync(batchClient, PoolId, applicationFiles);   //SSK

                // Create the job that will run the tasks.
                await CreateJobAsync(batchClient, JobId, PoolId);

                // Add the tasks to the job. We need to supply a container shared access signature for the
                // tasks so that they can upload their output to Azure Storage.

                //SSK
                //await AddTasksAsync(batchClient, JobId, inputFiles, outputContainerSasUrl);
                await AddTasksAsync(batchClient, JobId, outputContainerSasUrl,blobClient);

                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete
                await MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(30));

                timer.Stop();
                Console.WriteLine();
                log.Info($"Sample end: {DateTime.Now.ToString()}");
                log.Info($"Elapsed time: {timer.Elapsed.ToString()}");

                // Clean up Batch resources (if the user so chooses)
                //await batchClient.JobOperations.DeleteJobAsync(JobId);
                //await batchClient.PoolOperations.DeletePoolAsync(PoolId); //SSK

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error : " + ex.Message);
            }
            finally
            {
                batchClient.Close();
                batchClient.Dispose();
            }
        }
        
    }

    private void LoadEnvironmentVariables()
    {
        BatchAccountName= GetEnvironmentVariable("BatchAccountName");
        BatchAccountKey= GetEnvironmentVariable("BatchAccountKey");
        BatchAccountUrl= GetEnvironmentVariable("BatchAccountUrl");
        StorageAccountName= GetEnvironmentVariable("StorageAccountName");
        StorageAccountKey= GetEnvironmentVariable("StorageAccountKey");
        appContainerName = GetEnvironmentVariable("appContainerName");
        outputContainerName = GetEnvironmentVariable("outputContainerName");
        StagingAccName = GetEnvironmentVariable("StagingAccountName");
        StagingAccKey = GetEnvironmentVariable("StagingStorageAccountKey");
        stagingOutputName = GetEnvironmentVariable("StagingOutputContainerName");
        PoolId = GetEnvironmentVariable("BatchPoolName") ;
        
    } 
    /// <summary>
    /// Creates a container with the specified name in Blob storage, unless a container with that name already exists.
    /// </summary>
    /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
    /// <param name="containerName">The name for the new container.</param>
    /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
    private async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
    {
        CloudBlobContainer container = blobClient.GetContainerReference(containerName);

        if (await container.CreateIfNotExistsAsync())
        {
            log.Info($"Container {containerName} created.");
        }
        else
        {
            log.Info($"Container {containerName} exists, skipping creation." );
        }
    }

    /// <summary>
    /// Returns a shared access signature (SAS) URL providing the specified permissions to the specified container.
    /// </summary>
    /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
    /// <param name="containerName">The name of the container for which a SAS URL should be obtained.</param>
    /// <param name="permissions">The permissions granted by the SAS URL.</param>
    /// <returns>A SAS URL providing the specified access to the container.</returns>
    /// <remarks>The SAS URL provided is valid for 2 hours from the time this method is called. The container must
    /// already exist within Azure Storage.</remarks>
    private string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
    {
        // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
        // so the shared access signature becomes valid immediately
        SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
        {
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
            Permissions = permissions
        };

        // Generate the shared access signature on the container, setting the constraints directly on the signature
        CloudBlobContainer container = blobClient.GetContainerReference(containerName);
        string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

        // Return the URL string for the container, including the SAS token
        return String.Format("{0}{1}", container.Uri, sasContainerToken);
    }

    /// <summary>
    /// Uploads the specified files to the specified Blob container, returning a corresponding
    /// collection of <see cref="ResourceFile"/> objects appropriate for assigning to a task's
    /// <see cref="CloudTask.ResourceFiles"/> property.
    /// </summary>
    /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
    /// <param name="containerName">The name of the blob storage container to which the files should be uploaded.</param>
    /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
    /// <returns>A collection of <see cref="ResourceFile"/> objects.</returns>
    private async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
    {
        List<ResourceFile> resourceFiles = new List<ResourceFile>();

        foreach (string filePath in filePaths)
        {
            resourceFiles.Add(await UploadFileToContainerAsync(blobClient, inputContainerName, filePath));
        }

        return resourceFiles;
    }

    /// <summary>
    /// Uploads the specified file to the specified Blob container.
    /// </summary>
    /// <param name="inputFilePath">The full path to the file to upload to Storage.</param>
    /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
    /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
    /// <returns>A <see cref="Microsoft.Azure.Batch.ResourceFile"/> instance representing the file within blob storage.</returns>
    private async Task<ResourceFile> UploadFileToContainerAsync(CloudBlobClient blobClient, string containerName, string filePath)
    {
        log.Info($"Uploading file {filePath} to container...");
        log.Info($"container {containerName}..."  );
        string blobName = Path.GetFileName(filePath);

        CloudBlobContainer container = blobClient.GetContainerReference(containerName);
        CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
        await blobData.UploadFromFileAsync(filePath);

        // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
        // so the shared access signature becomes valid immediately
        SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
        {
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
            Permissions = SharedAccessBlobPermissions.Read
        };

        // Construct the SAS URL for blob
        string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
        string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

        return new ResourceFile(blobSasUri, blobName);
    }

    /// <summary>
    /// Creates a <see cref="CloudPool"/> with the specified id and configures its StartTask with the
    /// specified <see cref="ResourceFile"/> collection.
    /// </summary>
    /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
    /// <param name="poolId">The id of the <see cref="CloudPool"/> to create.</param>
    /// <param name="resourceFiles">A collection of <see cref="ResourceFile"/> objects representing blobs within
    /// a Storage account container. The StartTask will download these files from Storage prior to execution.</param>
    /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
    private async Task CreatePoolIfNotExistAsync(BatchClient batchClient, string poolId, IList<ResourceFile> resourceFiles)
    {
        CloudPool pool = null;
        try
        {
            log.Info($"Creating pool {poolId}...");

            // Create the unbound pool. Until we call CloudPool.Commit() or CommitAsync(), no pool is actually created in the
            // Batch service. This CloudPool instance is therefore considered "unbound," and we can modify its properties.
            pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                targetDedicated: 2,                                                         // 3 compute nodes
                virtualMachineSize: "small",                                                // single-core, 1.75 GB memory, 225 GB disk
                cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));   // Windows Server 2012 R2

            // Create and assign the StartTask that will be executed when compute nodes join the pool.
            // In this case, we copy the StartTask's resource files (that will be automatically downloaded
            // to the node by the StartTask) into the shared directory that all tasks will have access to.
            pool.StartTask = new StartTask
            {
                // Specify a command line for the StartTask that copies the task application files to the
                // node's shared directory. Every compute node in a Batch pool is configured with a number
                // of pre-defined environment variables that can be referenced by commands or applications
                // run by tasks.

                // Since a successful execution of robocopy can return a non-zero exit code (e.g. 1 when one or
                // more files were successfully copied) we need to manually exit with a 0 for Batch to recognize
                // StartTask execution success.
                CommandLine = "cmd /c (robocopy %AZ_BATCH_TASK_WORKING_DIR% %AZ_BATCH_NODE_SHARED_DIR%\\) ^& IF %ERRORLEVEL% LEQ 1 exit 0",
                ResourceFiles = resourceFiles,
                WaitForSuccess = true
            };

            await pool.CommitAsync();
        }
        catch (BatchException be)
        {
            // Swallow the specific error code PoolExists since that is expected if the pool already exists
            if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
            {
                log.Info($"The pool {poolId} already existed when we tried to create it");
            }
            else
            {
                throw; // Any other exception is unexpected
            }
        }
    }

    /// <summary>
    /// Creates a job in the specified pool.
    /// </summary>
    /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
    /// <param name="jobId">The id of the job to be created.</param>
    /// <param name="poolId">The id of the <see cref="CloudPool"/> in which to create the job.</param>
    /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
    private async Task CreateJobAsync(BatchClient batchClient, string jobId, string poolId)
    {
        try
        {
            log.Info($"Creating job {jobId}...");

            //IPagedEnumerable<CloudJob> jobList = batchClient.JobOperations.ListJobs();
            //bool isCloudJobExists = false;

            //if (jobList.Count() > 0)
            //{
            //    foreach (CloudJob cloudJob in jobList)
            //    {
            //        if (cloudJob.Id == jobId)
            //        {
            //            isCloudJobExists = true;
            //            Console.WriteLine("The job {0} already existed when we tried to create it", jobId);
            //        }
            //    }
            //}

            //if(!isCloudJobExists)
            {
                CloudJob job = batchClient.JobOperations.CreateJob();
                job.Id = jobId;
                job.PoolInformation = new PoolInformation { PoolId = poolId };

                await job.CommitAsync();
            }
        }
        catch (BatchException be)
        {
            log.Info($"Error occured while trying to process CloudJob: {be.Message} ");
            throw; // Any other exception is unexpected

        }
    }

    /// <summary>
    /// Creates tasks to process each of the specified input files, and submits them to the
    /// specified job for execution.
    /// </summary>
    /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
    /// <param name="jobId">The id of the job to which the tasks should be added.</param>
    /// <param name="inputFiles">A collection of <see cref="ResourceFile"/> objects representing the input files to be
    /// processed by the tasks executed on the compute nodes.</param>
    /// <param name="outputContainerSasUrl">The shared access signature URL for the container within Azure Storage that
    /// will receive the output files created by the tasks.</param>
    /// <returns>A collection of the submitted tasks.</returns>
    //private static async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
    private async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, string jobId, string outputContainerSasUrl, CloudBlobClient blobClient)
    {
        log.Info($"Adding tasks to job {jobId}...");

        // Create a collection to hold the tasks that we'll be adding to the job
        List<CloudTask> tasks = new List<CloudTask>();

        // Create each of the tasks. Because we copied the task application to the
        // node's shared directory with the pool's StartTask, we can access it via
        // the shared directory on whichever node each task will run.
        try
        {
            foreach (string inputContainer in inputContainerList)
            {
                CloudBlobContainer container = blobClient.GetContainerReference(inputContainer);

                //Check whether blob has files or not, if not skip task creation.

                IEnumerable<IListBlobItem> item = container.ListBlobs(prefix: null, useFlatBlobListing: true);
                if (item != null && item.Count() == 0)
                {
                    log.Info($"Skipping job creation, as there is no files in blob {inputContainer}...");
                    continue;
                }
                // try
                // {
                //     var filelist = item.AsEnumerable().Select(r => string.IsNullOrEmpty(r.Uri.AbsoluteUri)).ToList();
                // }
                // catch (Exception ex)
                // {
                //     log.Info($"Skipping job creation, as there is no files in blob [{inputContainer}]..." );
                //     continue;
                // }

                log.Info($"Task creation,for the  blob [{inputContainer}]..." );
                string taskId = "CatchmentAvgTask_" + inputContainer +  "_" + DateTime.Now.Ticks.ToString();
                //string taskId = "taskCatchment_" + inputFiles.IndexOf(inputFile) + DateTime.UtcNow.Ticks.ToString();
                string taskCommandLine = String.Format("cmd /c %AZ_BATCH_NODE_SHARED_DIR%\\ProcessCatchment.exe {0} {1} {2} {3} {4} {5} {6}", inputContainer, outputContainerName, StorageAccountName, StorageAccountKey,StagingAccName, StagingAccKey, stagingOutputName); 
                //StagingAccName, StagingAccKey, stagingOutputName
                
                CloudTask task = new CloudTask(taskId, taskCommandLine);
                //task.ResourceFiles = new List<ResourceFile> { inputFile };
                tasks.Add(task);
                //await batchClient.JobOperations.ReactivateTaskAsync(jobId, taskId);
                
            }

            // Add the tasks as a collection opposed to a separate AddTask call for each. Bulk task submission
            // helps to ensure efficient underlying API calls to the Batch service.
            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);
            log.Info($"Task execution is done..." );
        }
        catch (BatchException be)
        {
            log.Info($"Error occured while trying to process task: {be.Message} ");
            throw; // Any other exception is unexpected

        }
        catch (Exception be)
        {
            log.Info($"Error occured while trying to process task: {be.Message} ");
            throw; // Any other exception is unexpected

        }
        return tasks;
    }

    /// <summary>
    /// Monitors the specified tasks for completion and returns a value indicating whether all tasks completed successfully
    /// within the timeout period.
    /// </summary>
    /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
    /// <param name="jobId">The id of the job containing the tasks that should be monitored.</param>
    /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
    /// <returns><c>true</c> if all tasks in the specified job completed with an exit code of 0 within the specified timeout period, otherwise <c>false</c>.</returns>
    private async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
    {
        bool allTasksSuccessful = true;
        const string successMessage = "All tasks reached state Completed.";
        const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

        // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
        // specify that only the "id" property of each task should be populated. Using a detail level for
        // all list operations helps to lower response time from the Batch service.
        ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
        List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(JobId, detail).ToListAsync();

        log.Info($"Awaiting task completion, timeout in {timeout.ToString()}...");

        // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
        // reach the Completed state.
        TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
        try
        {
            await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
        }
        catch (TimeoutException)
        {
            await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
            Console.WriteLine(failureMessage);
            return false;
        }

        await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

        // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
        // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
        // or return a non-zero exit code.

        // Update the detail level to populate only the task id and executionInfo properties.
        // We refresh the tasks below, and need only this information for each task.
        detail.SelectClause = "id, executionInfo";

        foreach (CloudTask task in tasks)
        {
            // Populate the task's properties with the latest info from the Batch service
            await task.RefreshAsync(detail);

            if (task.ExecutionInformation.SchedulingError != null)
            {
                // A scheduling error indicates a problem starting the task on the node. It is important to note that
                // the task's state can be "Completed," yet still have encountered a scheduling error.

                allTasksSuccessful = false;

                log.Info($"WARNING: Task {task.Id} encountered a scheduling error : {task.ExecutionInformation.SchedulingError.Message}");
            }
            else if (task.ExecutionInformation.ExitCode != 0)
            {
                // A non-zero exit code may indicate that the application executed by the task encountered an error
                // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                // your implementation of error checking may differ from this example.

                allTasksSuccessful = false;

                log.Info($"WARNING: Task {task.Id} returned a non-zero exit code - this may indicate task execution or completion failure.");
            }
        }

        if (allTasksSuccessful)
        {
            log.Info($"Success! All tasks completed successfully within the specified timeout period.");
        }

        return allTasksSuccessful;
    }

    /// <summary>
    /// Downloads all files from the specified blob storage container to the specified directory.
    /// </summary>
    /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
    /// <param name="containerName">The name of the blob storage container containing the files to download.</param>
    /// <param name="directoryPath">The full path of the local directory to which the files should be downloaded.</param>
    /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
    private async Task DownloadBlobsFromContainerAsync(CloudBlobClient blobClient, string containerName, string directoryPath)
    {
        log.Info($"Downloading all files from container {containerName}...");
        //List<string> inputFiles = new List<string>();
        // Retrieve a reference to a previously created container
        CloudBlobContainer container = blobClient.GetContainerReference(containerName);

        // Get a flat listing of all the block blobs in the specified container
        foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
        {
            // Retrieve reference to the current blob
            CloudBlob blob = (CloudBlob)item;

            // Save blob contents to a file in the specified folder
            string localOutputFile = Path.Combine(directoryPath, blob.Name);
            await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            //inputFiles.Add(localOutputFile);

        }

        log.Info($"All files downloaded to {directoryPath}");
        //return inputFiles;
    }

    /// <summary>
    /// Get all lists of file to be downloaded into compute nodes while doing pool.StartTask
    /// </summary>
    /// <param name="blobClient"></param>
    /// <param name="containerName"></param>
    /// <returns></returns>
    private List<ResourceFile> GetListOfBlobResources(CloudBlobClient blobClient, string containerName)
    {
        log.Info($"Get all file lists from container {containerName}...");
        List<ResourceFile> resourceFiles = new List<ResourceFile>();
        // Retrieve a reference to a previously created container
        CloudBlobContainer container = blobClient.GetContainerReference(containerName);

        // Get a flat listing of all the block blobs in the specified container
        foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
        {
            // Retrieve reference to the current blob
            CloudBlob blobData = (CloudBlob)item;

            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            resourceFiles.Add(new ResourceFile(blobSasUri, blobData.Name));
        }

        log.Info($"All files downloaded to {resourceFiles.Count.ToString()}");
        return resourceFiles;
    }

    /// <summary>
    /// Deletes the container with the specified name from Blob storage, unless a container with that name does not exist.
    /// </summary>
    /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
    /// <param name="containerName">The name of the container to delete.</param>
    /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
    private async Task DeleteContainerAsync(CloudBlobClient blobClient, string containerName)
    {
        CloudBlobContainer container = blobClient.GetContainerReference(containerName);

        if (await container.DeleteIfExistsAsync())
        {
            log.Info($"Container {containerName} deleted.");
        }
        else
        {
            log.Info($"Container {containerName} does not exist, skipping deletion.");
        }
    }

    /// <summary>
    /// Processes all exceptions inside an <see cref="AggregateException"/> and writes each inner exception to the console.
    /// </summary>
    /// <param name="aggregateException">The <see cref="AggregateException"/> to process.</param>
    public void PrintAggregateException(AggregateException aggregateException)
    {
        // Flatten the aggregate and iterate over its inner exceptions, printing each
        foreach (Exception exception in aggregateException.Flatten().InnerExceptions)
        {
            Console.WriteLine(exception.ToString());
            Console.WriteLine();
        }
    }

    public static string GetEnvironmentVariable(string name)
    {
        return  System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
    }
}