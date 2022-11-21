using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace ifis_patbase_importer
{
    public class XRaw : XText
    {
        public XRaw(string text) : base(text) { }
        public XRaw(XText text) : base(text) { }

        public override void WriteTo(System.Xml.XmlWriter writer)
        {
            writer.WriteRaw(this.Value);
        }
    }
}
