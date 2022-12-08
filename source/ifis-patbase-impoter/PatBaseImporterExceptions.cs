using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace ifis_patbase_importer
{
    public class KeyDataNotFoundException : Exception
    {
        public XElement Record { get; }
        public bool Log { get; }
        public KeyDataNotFoundException(string message): base(message) { }

        public KeyDataNotFoundException(string message, XElement record, bool log) : this(message)
        {
            Record = record;
            Log = log;
        }
    }
}
