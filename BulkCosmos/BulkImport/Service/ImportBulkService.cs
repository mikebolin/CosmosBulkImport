using BulkCosmos.Import.Model;
using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace BulkExecutors.ImportExecutor
{
    public class ImportBulkService : IImportBulkService
    {
        public BulkImportModel _model { get; set; }
        public ImportBulkService()  
        { 
            _model = new BulkImportModel(); //pull values from IConfig for setup and conenction 
        }
        public async Task RunBulkImportAsync()
        {
            await _model.RecreateDBOnStart(bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"])).ConfigureAwait(true);
            await RunBatches().ConfigureAwait(true);
            await _model.ShouldRemoveOnFinish(bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"])).ConfigureAwait(true);
        }
        public async Task RunBatches() {
            var token = new CancellationTokenSource().Token;
            for (int i = 0; i < _model.numBatches; i++)
            {
                _model.GenerateDummyDocuments(i);

                var tasks = new List<Task>();
                tasks.Add(Task.Run(async () => {
                    do
                    {
                        try
                        {
                            _model.resp = await _model._bulkExecutor.BulkImportAsync(
                                documents: _model.importDocs,
                                enableUpsert: true,
                                disableAutomaticIdGeneration: true,
                                maxConcurrencyPerPartitionKeyRange: null,
                                maxInMemorySortingBatchSize: null, 
                                cancellationToken: token
                                );
                        }
                        catch (Exception e) { Debug.WriteLine("Exception: {0}", e); break; }
                    } while (_model.resp.NumberOfDocumentsImported < _model.importDocs.Count);
                    _model.LogSummary("Batch Summry " + i.ToString(), _model.resp.NumberOfDocumentsImported, _model.resp.TotalTimeTaken.TotalSeconds, _model.resp.TotalRequestUnitsConsumed);
                    _model.LogAverageRUPerDocument(_model.resp.TotalRequestUnitsConsumed, _model.resp.NumberOfDocumentsImported);
                }, token));
                await Task.WhenAll(tasks).ConfigureAwait(true);
            }
            _model.LogSummary("Overall Summary", _model.DocsInserted, _model.ElapsedSeconds, _model.RUConsumed);
        }
    }
}

