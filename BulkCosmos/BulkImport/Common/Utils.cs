using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkExecutors.ImportExecutor
{
    class Utils
    {
        static internal DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null) { return null; }
            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName)).Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }
        static internal Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }
        static internal async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName,
            string collectionName, int collectionThroughput)
        {
            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition { Paths = new Collection<string> { ConfigurationManager.AppSettings["CollectionPartitionKey"] } };
            DocumentCollection collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };
            try
            {
                collection = await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(databaseName), collection, new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e) { throw e; }
            return collection;
        }
        static internal String GenerateDocuments(String id, String partitionKeyProperty, object parititonKeyValue)
        {
            return "{\n" + "    \"id\": \"" + id + "\",\n" + "    \"" + partitionKeyProperty + "\": \"" + parititonKeyValue + "\",\n" + "    \"Name\": \"TestDoc\",\n" + "    \"description\": \"1.99\",\n" + "    \"f1\": \"3hrkjh3h4h4h3jk4h\",\n" + "    \"f2\": \"dhfkjdhfhj4434434\",\n" + "    \"f3\": \"nklfjeoirje434344\",\n" + "    \"f4\": \"pjfgdgfhdgfgdhbd6\",\n" + "    \"f5\": \"gyuerehvrerebrhjh\",\n" + "    \"f6\": \"3434343ghghghgghj\"" + "}";
        }
    }

    
}
