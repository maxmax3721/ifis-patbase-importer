using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace ifis_patbase_importer
{
    public class SpecialCharacters
    {
        
        static KeyValuePair<string, string>[] characterReplacements = new KeyValuePair<string, string>[]
            {
                new KeyValuePair<string,string>("Î±", "α"),
                new KeyValuePair<string,string>("Î²", "β"),
                new KeyValuePair<string,string>("Î³", "γ"),
                new KeyValuePair<string,string>("IÂ´", "δ"),
                new KeyValuePair<string,string>("Îµ", "ε"),
                new KeyValuePair<string,string>("Î¶", "ζ"),
                new KeyValuePair<string,string>("Î·", "η"),
                new KeyValuePair<string,string>("Î¹", "ι"),
                new KeyValuePair<string,string>("Îº", "κ"),
                new KeyValuePair<string,string>("Î»", "λ"),
                new KeyValuePair<string,string>("Î¼", "μ"),
                new KeyValuePair<string,string>("Ïƒ", "σ"),
                new KeyValuePair<string,string>("Ï‰", "ω"),
                new KeyValuePair<string,string>("â€²", "ʹ"),
                new KeyValuePair<string,string>("â€³", "ʺ"),
                new KeyValuePair<string,string>("Â®", "®"),
                new KeyValuePair<string,string>("Â±", "±"),
                new KeyValuePair<string,string>("Â©", "©"),
                new KeyValuePair<string,string>("â€“", "-"),
                new KeyValuePair<string, string>("â€”","-"),
                new KeyValuePair<string,string>("Ã—", "x"),
                new KeyValuePair<string,string>("Â°", "°")
            };

        public static string RemoveControlChars(object Abstract)
        {
            if (Abstract != System.DBNull.Value)
            {
                string String = (string)Abstract;
                return new string(String.Where(c => !char.IsControl(c)).ToArray());
            }
            else
            {
                return "";
            }
            
        }

        public static string ReplaceCharacters(string theString)
        {
            

            foreach (var replace in characterReplacements)
            {
                theString = theString.Replace(replace.Key, replace.Value);
            }
            return theString;
        }

        
        
    }
}
