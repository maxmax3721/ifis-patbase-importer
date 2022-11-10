using System;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace ifis_patbase_impoter
{
    public static class ElsExtensions
    {
        static ElsExtensions()
        {
        }

        private static readonly ReplaceSpec[] markUpToReplace = new[]
        {
            new ReplaceSpec("i", "i", "italic"),
            new ReplaceSpec("b", "b", "bold"),
            new ReplaceSpec("b [^>]*", "b", "bold"),
            new ReplaceSpec("font [^>]*small *- *caps[^>]*", "font", null, s => s.ToUpper()),
            new ReplaceSpec("smallcaps", "smallcaps", null, s => s.ToUpper()),
            new ReplaceSpec("smallcaps [^>]*", "smallcaps", null, s => s.ToUpper()),
            new ReplaceSpec("imageobject [^>]*", "imageobject", null),
            new ReplaceSpec("superscript", "superscript", "sup"),
            new ReplaceSpec("sup [^>]*", "sup", "sup"),
            new ReplaceSpec("sub [^>]*", "sub", "sub"),
            new ReplaceSpec("subscript", "subscript", "sub"),
            new ReplaceSpec("ce:hsp [^>]*", "ce:hsp", null),
            new ReplaceSpec("ce:glyph [^>]*", "ce:glyph", null),
            new ReplaceSpec("math [^>]*", "math [^>]*", null, s => ""),
            new ReplaceSpec("mml:math [^>]*", "mml:math [^>]*", null, s => ""),
            new ReplaceSpec("sec sec-type=[^>]*", "sec sec-type=[^>]*", null, s => ""),
            new ReplaceSpec("br ", null, null),
            new ReplaceSpec("url [^>]*", "url [^>]*", null),
            new ReplaceSpec("http: [^>]*", "http:", null),
        };

        public static string ApplyMarkupReplacements(this string text)
        {
            var updatedMarkup = markUpToReplace.Aggregate(text, (s, replaceSpec) =>
                replaceSpec.SearchTagRegex.Replace(s,
                    m => replaceSpec.FinalTag == null
                         ? replaceSpec.ContentTransform(m.Groups[1].Value)
                         : "<" + replaceSpec.FinalTag + ">" + replaceSpec.ContentTransform(m.Groups[1].Value) + "</" + replaceSpec.FinalTag + ">"));

            return updatedMarkup;
        }

        public static XRaw GetElsContent(this object data)
        {
            return new XRaw(data.ToString().ApplyMarkupReplacements());
        }

        private class ReplaceSpec
        {
            public Regex SearchTagRegex;
            public Func<string, string> ContentTransform = s => s;
            public string FinalTag;

            public ReplaceSpec(string openingTagRegex, string closingTagRegex, string finalTag = null, Func<string, string> contentTransform = null)
            {
                SearchTagRegex = closingTagRegex != null
                    ? new Regex("<" + openingTagRegex + ">(.*?)</" + closingTagRegex + ">")
                    : new Regex("<" + openingTagRegex + "/>");
                FinalTag = finalTag;
                ContentTransform = contentTransform ?? ContentTransform;
            }
        }

    }
}