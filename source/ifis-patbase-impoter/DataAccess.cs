using CommandLine;
using CsvHelper;
using Org.BouncyCastle.Crypto;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Xml.Linq;

namespace ifis_patbase_importer
{
    public static class DataAccess
    {
        public static bool IsNullorEmptyString(object content)
        {
            return ((content == System.DBNull.Value) || ((content is string) && string.IsNullOrEmpty((string)content))) || ((content is int) && ((int)content == 0));
        }

        public static IDictionary<string, object> ToIDictionary(this DataRow dr)
        {
            return dr.Table.Columns
              .Cast<DataColumn>()
              .ToDictionary(c => c.ColumnName, c => dr[c]);
        }

        internal static Func<IDictionary<string, object>, object> SpecificDataAccessor(DataSet ds)
        {
            // Do whatever you can one time in this function
            var otherTableToQuery = ds.Tables["SomeTable"].AsEnumerable();

            // ...rather than in this function that you are returning
            return row =>
            {
                var sourceKey = row["someColumn"];
                if (sourceKey is int && ((int)sourceKey) > 3)
                    return otherTableToQuery.Where(someTableRow => someTableRow["SomeColumn"] == row["otherTableKey"]);
                else
                    return "Whatever";
            };
        }


        internal static Func<XElement, IDictionary<string, object[]>> PublicationAccessor(DataSet sourceDataSet)
        {
            return record =>
            {
                var pubTitle = GetPubTitle(record, sourceDataSet);
                var journal_id = GetJournalID(pubTitle, record, sourceDataSet);
                var publisher_id = System.DBNull.Value; //TODO

                return new Dictionary<string, object[]>()
                {
                    {"publication_title",new [] {publisher_id} },
                    {"journal_id",new [] {journal_id} },
                    {"publisher_id",new [] {publisher_id} }
                };
            };

        }

        private static object GetJournalID(object pubTitle, XElement record, DataSet sourceDataSet)
        {
            var journalLUTRow = sourceDataSet.Tables["journal_id_lut"].Rows.Find(pubTitle.ToString());

            if (journalLUTRow != null)
            {
                return journalLUTRow["journal_ID"];
            }
            else
            {
                //throw new KeyDataNotFoundException("No journal_id found", record);
                return System.DBNull.Value;
            }
        }

        private static object GetPubTitle(XElement record, DataSet sourceDataSet)
        {
            var countryCode = record.Element("CountryCode").Value;

            if (countryCode != null)
            {
                var kindCode = record.Element("KindCode").Value;
                var pubTitleLUTRow = sourceDataSet.Tables["publication_title_lut"].Rows.Find(new[] { countryCode, kindCode });

                if (pubTitleLUTRow != null)
                {
                    return pubTitleLUTRow["publication_title"];
                }
                else
                {
                    pubTitleLUTRow = sourceDataSet.Tables["publication_title_lut"].Rows.Find(new[] { countryCode, "" });

                    if (pubTitleLUTRow != null)
                    {
                        return pubTitleLUTRow["publication_title"];
                    }
                    else
                    {
                        throw new Exception($"Country Code {countryCode} not found in publication_title_lut");
                    }
                }
            }
            else
            {
                throw new Exception($"Country Code null");
            }
        }

        internal static Func<XElement, IDictionary<string, object[]>> LabelAccessor(Options opts)
        {
            var Index = DatabaseMethods.GetMaxPbLabelInt(opts);

            return record =>
            {
                Index++;
                var Label = "PB" + Index.ToString("000000");

                return new Dictionary<string, object[]>
                {
                    {"label", new [] { Label } },
                    {"accession_number", new [] {Label} }
                };
            };
        }

        internal static Func<XElement, IDictionary<string, object[]>> TitleAccessor()
        {
            return record =>
            {
                object engTitle = System.DBNull.Value;
                object forTitle = System.DBNull.Value;

                var engTitleElement = record.Element("Titles").Elements("Title").FirstOrDefault(el => el.Attribute("lang").Value == "en");

                if (engTitleElement == null)
                {
                    var mtTitleElement = record.Element("Titles").Elements("Title").FirstOrDefault(el => el.Attribute("lang").Value == "en");
                    if (mtTitleElement == null)
                    {
                        var forTitleElement = record.Element("Titles").Elements("Title").FirstOrDefault();
                        {
                            if (forTitleElement == null)
                            {
                                throw new KeyDataNotFoundException("No Valid Title Element Found", record);
                            }
                            else
                            {
                                forTitle = forTitleElement.Value;
                            }
                        }
                    }
                    else
                    {
                        engTitle = mtTitleElement.Value;
                    }
                }
                else
                {
                    engTitle = engTitleElement.Value;
                }

                return new Dictionary<string, object[]>()
                {
                    { "english_title", new[] { engTitle } },
                    { "foreign_title", new[] { forTitle } }
                };
            };
        }

        internal static Func<XElement, object> PatentNumAccessor()
        {
            return record =>
            {
                object patNum = System.DBNull.Value;

                var PatNumAttribute = record.Attribute("pn");
                var KindCodeElement = record.Element("KindCode");

                if ((PatNumAttribute != null) & (KindCodeElement != null))
                {
                    patNum = PatNumAttribute.Value.ToString() + "" + KindCodeElement.Value.ToString();
                }
                else
                {
                    throw new KeyDataNotFoundException("No Patent Number Found", record);
                }

                return patNum;
            };
        }

        internal static Func<XElement, object> PatentPriorityAccessor()
        {
            return record =>
            {
                var priorityElements = record.Element("Priorities").Elements("Priority");

                if(priorityElements != null)
                {
                    var minPriorityDate = priorityElements.Min(el => el.Element("Date").Value);
                    var minPriorityDateElement = priorityElements.Where(el => el.Element("Date").Value == minPriorityDate).FirstOrDefault();
                    return minPriorityDateElement.Element("Number").Value + " (" + minPriorityDateElement.Element("Date").Value + ")";
                }
                else
                {
                    return System.DBNull.Value;
                }
            };
        }

        internal static Func<XElement, IDictionary<string, object[]>> PublicationDateFieldAccessor()
        {
            return record =>
            {
                var pubDate = DateTime.ParseExact(record.Element("PublicationDate").Value, "yyyyMMdd", null);

                return new Dictionary<string, object[]>()
                {
                    {"date_published", new[] { pubDate.ToString("yyyy-MM-dd") } },
                    {"publication_date_year", new [] { pubDate.Year.ToString()} },
                    {"publication_date_month", new [] { pubDate.Month.ToString() } },
                    {"publication_date_day", new [] { pubDate.Day.ToString()}}
                };

            };
        }

        internal static Func<XElement, IDictionary<string, object[]>> AuthorsAccessor(DataSet sourceDataSet)
        {
            return record =>
            {
                var authors = new List<object>();
                var tempAuthTyp = new List<object>();
                var tempEntryNo = new List<object>();
                var tempAddress = new List<object>();

                var AssigneeElements = record.Element("AssigneeDetails") != null ? record.Element("AssigneeDetails").Elements("Assignee"): null;
                var InventorElements = record.Element("InventorDetails") != null? record.Element("InventorDetails").Elements("Inventor"): null;

                var entryNo = 1;
                foreach (XElement Assignee in AssigneeElements)
                {
                    var author = Assignee.Element("Name").Value;
                    object address = Assignee.Element("Address") != null ? Assignee.Element("Address").Value : null;

                    authors.Add(author);
                    tempAuthTyp.Add("pa");
                    tempEntryNo.Add(entryNo.ToString());
                    tempAddress.Add(address);

                    entryNo++;
                }

                entryNo = 1;
                foreach (XElement Inventor in InventorElements)
                {
                    var author = Inventor.Element("Name").Value;
                    object address = Inventor.Element("Address") != null ? Inventor.Element("Address").Value : null;

                    authors.Add(author);
                    tempAuthTyp.Add("pi");
                    tempEntryNo.Add(entryNo.ToString());
                    tempAddress.Add(address);

                    entryNo++;
                }

                return new Dictionary<string, object[]>()
                {
                    {"author_name", authors.ToArray() },
                    {"TEMPORARY_author_type", tempAuthTyp.ToArray()  },
                    {"TEMPORARY_entry_no", tempEntryNo.ToArray() },
                    {"TEMPORARY_address", tempAddress.ToArray() }
                };

            };

        }

        public static DataTable ToDataTable(this XElement element)
        {
            DataSet ds = new DataSet();
            string rawXml = element.ToString();
            ds.ReadXml(new StringReader(rawXml));
            return ds.Tables[0];
        }

        internal static Func<IDictionary<string, DataRow[]>, IDictionary<string, object[]>> AuthorIDAccessor(DataSet sourceDataSet)
        {

            return newRowsForAbstract =>
            {
                var AuthorRows = newRowsForAbstract["authors"];
                var authorIDs = new List<object>();
                var addresses = new List<object>();

                foreach(DataRow row in AuthorRows)
                {
                    if (!IsNullorEmptyString(row["TEMPORARY_address"]))
                    {
                        authorIDs.Add(row["author_id"]);
                        addresses.Add(row["TEMPORARY_address"]);
                    }
                }

                return new Dictionary<string, object[]>()
                {
                    {"author_id", authorIDs.ToArray() },
                    {"address", addresses.ToArray() }
                };
            };
        }

        internal static Func<XElement, IDictionary<string, object[]>> LanguageAccessor()
        {
            throw new NotImplementedException();
        }

        internal static Func<XElement, object[]> ThesaurusIDAccessor(DataSet sourceDataSet)
        {
            var codeNotFoundLogger = new CsvLogger("codeNotFoundInControlledMajorLUT.csv");
            codeNotFoundLogger.csvWriter.WriteHeader<CodeNotFoundInControlledMajorLUTLog>();
            codeNotFoundLogger.csvWriter.NextRecord();

            return record =>
            {
                var IDs = new List<object>();

                var IPCElements = record.Element("IPCs").Elements("IPC");
                var CPCElements = record.Element("CPCs").Elements("CPC");

                IDs.LookupIDsFromPatentCodes(IPCElements,sourceDataSet, codeNotFoundLogger);
                IDs.LookupIDsFromPatentCodes(CPCElements,sourceDataSet, codeNotFoundLogger);

                return IDs.ToArray();
            };
        }

        private static List<object> LookupIDsFromPatentCodes(this List<object> IDs, IEnumerable<XElement> Elements,DataSet sourceDataSet, CsvLogger codeNotFoundLogger)
        {

            if (Elements != null)
            {
                foreach (XElement CodeElement in Elements)
                {
                    var controlledMajorLUTRow = sourceDataSet.Tables["controlled_major_lut"].Rows.Find(CodeElement.Value);

                    if(controlledMajorLUTRow != null)
                    {
                        foreach (int i in Enumerable.Range(1, 7))
                        {
                            var colName = "ID" + i.ToString();
                            var ID = controlledMajorLUTRow[colName];
                            if (!IsNullorEmptyString(ID))
                            {
                                IDs.Add(ID);
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        var record = new CodeNotFoundInControlledMajorLUTLog { Code = CodeElement.Value };
                        codeNotFoundLogger.csvWriter.WriteRecord(record);
                        codeNotFoundLogger.csvWriter.NextRecord();
                        codeNotFoundLogger.csvWriter.Flush();
                    }

                }
            }

            return IDs;
        }

        internal static Func<XElement, object> GetDate()
        {
            return record => { return DateTime.Now.Date.ToString("yyyy-MM-dd"); };
        }

        internal static Func<XElement, IDictionary<string, object[]>> ClassificationCodeAccessor()
        {
            return record =>
            {
                var Codes = new List<object>();
                var CodeTypes = new List<object>();

                var IPCElements = record.Element("IPCs").Elements("IPC");
                var CPCElements = record.Element("CPCs").Elements("CPC");

                if (IPCElements != null)
                {
                    foreach (XElement IPCElement in IPCElements)
                    {
                        Codes.Add(IPCElement.Value);
                        CodeTypes.Add("IPC");
                    }
                }

                if (CPCElements != null)
                {
                    foreach (XElement CPCElement in CPCElements)
                    {
                        Codes.Add(CPCElement.Value);
                        CodeTypes.Add("IPC");
                    }
                }

                return new Dictionary<string, object[]>()
                {
                    {"code", Codes.ToArray()},
                    {"code_type", CodeTypes.ToArray()}
                };
            };
        }
    }
}
