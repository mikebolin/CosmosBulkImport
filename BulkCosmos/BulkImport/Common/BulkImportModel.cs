using BulkExecutors.ImportExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace BulkCosmos.Import.Model
{
    public class BulkImportModel
    {
        public IBulkExecutor _bulkExecutor { get; set; }
        public DocumentCollection dataCollection { get; set; }
        public DocumentClient client { get; set; }
        public BulkImportResponse resp { get; set; }
        public BulkImportModel()
        {
            DatabaseName = "";
            CollectionName = "";
            CollectionThroughput = 1;
            MaxRetryWaitTimeInSeconds = 9;
            MaxRetryAttemptsOnThrottledRequests = 30;
            partitionKey = "";
            importDocs = new List<string>();
            resp = null;
        }
        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }
        public int CollectionThroughput { get; set; }
        public ConnectionPolicy ConnectionPolicy = new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp };
        public int MaxRetryWaitTimeInSeconds { get; set; }
        public int MaxRetryAttemptsOnThrottledRequests { get; set; }
        public string partitionKey { get; set; }
        public long numDocs { get; set; }
        public int numBatches { get; set; }
        public long numDocPerBatch { get; set; }
        public List<string> importDocs { get; set; }
        public long DocsInserted { get; set; }
        public double RUConsumed { get; set; }
        public double ElapsedSeconds { get; set; }
        public async Task RecreateDBOnStart(bool shouldRecreate)
        {
            try
            {
                if (shouldRecreate)
                {
                    Database database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null) { await client.DeleteDatabaseAsync(database.SelfLink); }
                    database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });
                    Debug.WriteLine(String.Format("Creating collection {0} with {1} RU/s", CollectionName, CollectionThroughput));
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName, CollectionThroughput);
                }
                else
                {
                    dataCollection = Utils.GetCollectionIfExists(client, DatabaseName, CollectionName);
                    if (dataCollection == null) { throw new Exception("The data collection does not exist"); }
                }
                await SetupConnection().ConfigureAwait(true);
            }
            catch (Exception de) { Debug.WriteLine("Unable to initialize, exception message: {0}", de.Message); throw; }
        }
        public async Task SetupConnection()
        {
            // Set options 
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = MaxRetryWaitTimeInSeconds;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = MaxRetryAttemptsOnThrottledRequests;
            _bulkExecutor = new BulkExecutor(client, dataCollection);
            await _bulkExecutor.InitializeAsync();
            //pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;
        }
        public async Task ShouldRemoveOnFinish(bool shouldRecreate)
        {
            await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
        }
        public void SetSummaryVariables()
        {
            partitionKey = dataCollection.PartitionKey.Paths[0].Replace("/", "");
            numDocs = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToImport"]);
            numBatches = int.Parse(ConfigurationManager.AppSettings["NumberOfBatches"]);
            numDocPerBatch = (long)Math.Floor(((double)numDocs) / numBatches);
        }
        public void GenerateDummyDocuments(int batchNum) {
            long prefix = batchNum * numDocPerBatch;
            for (int j = 0; j < numDocPerBatch; j++)
            {
                string id = (prefix + j).ToString() + Guid.NewGuid().ToString();
                importDocs.Add(Utils.GenerateDocuments(id, partitionKey, (prefix + j).ToString()));
            }
        }
        public void LogAverageRUPerDocument(double RUConsumedForDoc, double DocsImported)
        {
            Debug.WriteLine(String.Format("Average RU consumption per document: {0}", (RUConsumedForDoc / DocsImported)));
            DocsInserted += resp.NumberOfDocumentsImported;
            RUConsumed += resp.TotalRequestUnitsConsumed;
            ElapsedSeconds += resp.TotalTimeTaken.TotalSeconds;
        }
        public void LogSummary(string Message, double numDocuments, double totalTime, double RUs)
        {
            Debug.WriteLine(Message);
            Debug.WriteLine("Inserted " + numDocuments);
            Debug.WriteLine("Writes/seconds " + Math.Round(numDocuments / totalTime));
            Debug.WriteLine("Total Time " + Math.Round(RUs / totalTime), totalTime);
        }

    }
}
