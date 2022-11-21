using CsvHelper;
using CsvHelper.Configuration;
using Google.Protobuf.WellKnownTypes;
using Org.BouncyCastle.Tls;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection.Emit;
using System.Text;
using System.Xml.Linq;

namespace ifis_patbase_importer
{
    public class CsvLogger
    {
        public string CSVPath;
        public StreamWriter writer;
        public CsvWriter csvWriter;

        
        public CsvLogger(string csvPath) { 
            CSVPath = csvPath;

            if (System.IO.File.Exists(CSVPath))
            {
                System.IO.File.Delete(CSVPath);
            }
            System.IO.File.Create(CSVPath).Dispose();
            
            writer = new StreamWriter(CSVPath);
            csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
        }
    }

    public class RecordLog
    {
        public string Label { get; set; }
        public string PublicationNumber { get; set; }
    }

    public class CodeNotFoundInControlledMajorLUTLog
    {
        public string Code { get; set; }
    }


}
