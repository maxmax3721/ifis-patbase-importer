using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace ifis_patbase_impoter
{
    public class KeyDataNotFoundException : Exception
    {
        public string CountryCode { get; }
        public string KindCode { get; }
        public string PatentNumber { get; }

        public KeyDataNotFoundException(string message): base(message) { }

        public KeyDataNotFoundException(string message, XElement record): this(message)
        {
            CountryCode = record.Element("CountryCode") != null ? record.Element("CountryCode").Value : null;
            KindCode = record.Element("KindCode") != null ? record.Element("KindCode").Value : null;
            PatentNumber = record.Attribute("pn") != null ? record.Attribute("pn").Value : null;
        }
    }
}
