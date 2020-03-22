using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BulkExecutors.ImportExecutor
{
    public interface IImportBulkService
    {
        Task RunBulkImportAsync();
    }
}
