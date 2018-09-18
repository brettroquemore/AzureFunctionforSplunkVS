using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System;

namespace AzureFunctionForSplunk
{
    public static class FaultProcessor
    {
        [FunctionName("FaultProcessor")]
        public static async Task Run(
            [QueueTrigger("transmission-faults", Connection = "AzureWebJobsStorage")]string fault,
            IBinder blobFaultBinder,
            TraceWriter log)
        {
            try
            {
                var faultData = JsonConvert.DeserializeObject<TransmissionFaultMessage>(fault);
                log.Info(fault);
                log.Info("Line 23");

                var blobReader = await blobFaultBinder.BindAsync<CloudBlockBlob>(
                        new BlobAttribute($"transmission-faults/{faultData.id}", FileAccess.ReadWrite));
                log.Info("Line 27");

                var json = await blobReader.DownloadTextAsync();
                log.Info("Line 30");

                try
                {
                    List<string> faultMessages = await Task<List<string>>.Factory.StartNew(() => JsonConvert.DeserializeObject<List<string>>(json));
                    log.Info("Line 35");
                    await Utils.obHEC(faultMessages, log);
                } catch
                {
                    log.Error($"FaultProcessor failed to send to Splunk: {faultData.id}");
                    return;
                }

                await blobReader.DeleteAsync();

                log.Info($"C# Queue trigger function processed: {faultData.id}");
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
                throw;
            }
        }
    }
}
