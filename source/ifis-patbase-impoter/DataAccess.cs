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
                
                var journalID = GetJournalID(record, sourceDataSet);
                object pubTitle;
                object publisherID;

                if(journalID != null)
                {
                    var journalRow = GetJournalRow(journalID, record, sourceDataSet);
                    pubTitle = journalRow["journal_name"];
                    publisherID = journalRow["publisher_id"];

                }
                else
                {
                    pubTitle = System.DBNull.Value;
                    publisherID = System.DBNull.Value;
                }



                return new Dictionary<string, object[]>()
                {
                    {"publication_title",new [] {pubTitle} },
                    {"journal_id",new [] {journalID} },
                    {"publisher_id",new [] {publisherID} }
                };
            };

        }

        private static object GetJournalID( XElement record, DataSet sourceDataSet)
        {
            var countryCode = record.Element("CountryCode").Value;
            var kindCode = record.Element("KindCode").Value;

            var journalLUTRow = sourceDataSet.Tables["journal_id_lut"].AsEnumerable().Where(dr => ((string)dr["CountryCode"] == countryCode) && ((string)dr["KindCode"] == kindCode));

            if (journalLUTRow.Any())
            {
                return journalLUTRow.First()["Journal_ID"];
            }
            else
            {
                journalLUTRow = sourceDataSet.Tables["journal_id_lut"].AsEnumerable().Where(dr => ((string)dr["CountryCode"] == countryCode) && ((string)dr["KindCode"] == ""));

                if(journalLUTRow.Any())
                {
                    return journalLUTRow.First()["Journal_ID"];
                }
                else
                {
                    throw new KeyDataNotFoundException("No journal_id found in journal_id_lut", record, true);
                }
            }
        }

        private static DataRow GetJournalRow(object journalID, XElement record, DataSet sourceDataSet)
        {
            var journalsRow = sourceDataSet.Tables["journals"].Rows.Find(journalID);
            if (journalsRow != null)
            {
                return journalsRow;
            }
            else
            {
                throw new KeyDataNotFoundException($"Journal ID {journalID} not found in grand_central.journals", record, true);
            }
        }

        internal static Func<XElement, IDictionary<string, object[]>> LabelAccessor(Options opts)
        {
            return record =>
            {
                //significantly slows process but means labels are consecutive even when a records is not imported
                var Index = DatabaseMethods.GetMaxPbLabelInt(opts);
                Index++;
                var Label = "PB" + Index.ToString("000000");

                return new Dictionary<string, object[]>
                {
                    {"label", new [] { Label } },
                    {"accession_number", new [] {Label} }
                };
            };
        }

        internal static Func<XElement, IDictionary<string, object[]>> TitleAccessor(DataSet sourceDataSet)
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
                                throw new KeyDataNotFoundException("No Valid Title Element Found", record, true);
                            }
                            else
                            {
                                var langrow = sourceDataSet.Tables["language_lut"].Rows.Find(forTitleElement.Attribute("lang").Value);

                                if(langrow != null)
                                {
                                    if (langrow["charset"].ToString() == "latin")
                                    {
                                        forTitle = forTitleElement.Value;
                                    }
                                    else
                                    {
                                        throw new KeyDataNotFoundException("No Valid Latin-Charset Title Element Found", record, true);
                                    }
                                }
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

        internal static Func<XElement, object> PatentNumAccessor(DataSet sourceDataSet)
        {
            return record =>
            {
                object patNum = System.DBNull.Value;

                var PatNumAttribute = record.Attribute("pn");
                var CountryCodeElement = record.Element("CountryCode");
                var KindCodeElement = record.Element("KindCode");

                try
                {
                    var GCKindCode = sourceDataSet.Tables["kind_code_lut"].Rows.Find(new object[] { CountryCodeElement.Value.ToString(), KindCodeElement.Value.ToString() });
                    patNum = PatNumAttribute.Value.ToString() + " " + KindCodeElement.Value.ToString();
                }
                catch
                {
                    throw new KeyDataNotFoundException("No Patent Number Found", record, true);
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

                if(authors.Count() == 0)
                {

                    Program.WriteDataNotFoundError(Console.Error, record, "No author elements found for Record");
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

        public static Func<IDictionary<string, DataRow[]>, IDictionary<string, object[]>> AuthorIDAccessor(DataSet sourceDataSet)
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

        public static Func<XElement, IDictionary<string, object[]>> LanguageAccessor(DataSet sourceDataSet)
        {
            return record =>
            {
                var summaryLanguages = record.Element("Abstracts").Elements("Abstract").Where(el => (el.Attribute("lang").Value != "mt")).Select(el => el.Attribute("lang").Value);
                var sourceLanguages = record.Element("Descriptions").Elements("Description").Where(el => (el.Attribute("lang").Value != "mt")).Select(el => el.Attribute("lang").Value);

                var Langs = new List<Object>();
                var LangCodes = new List<Object>();
                var EntryNos = new List<Object>();
                var SumSource = new List<Object>();

                var sumSource = "u";
                foreach (var langEnumerable in new[] { summaryLanguages, sourceLanguages })
                { 

                    var i = 0;
                    foreach (var lang in langEnumerable)
                    {
                        var langLUTRow = sourceDataSet.Tables["language_LUT"].Rows.Find(lang);

                        if (langLUTRow != null)
                        {
                            i++;
                            Langs.Add(langLUTRow["language"]);
                            LangCodes.Add(langLUTRow["gc_language_code"]);
                            EntryNos.Add(i);
                            SumSource.Add(sumSource);

                        }
                        else
                        {
                            Program.WriteError(Console.Error, "Language Code Not Found in Languages LUT", lang, "");
                        }
                    }
                    sumSource = "o";
                }

                return new Dictionary<string, object[]>()
                {
                    {"language", Langs.ToArray() },
                    {"language_code", LangCodes.ToArray() },
                    {"entry_no", EntryNos.ToArray() },
                    {"summary_source", SumSource.ToArray() }
                };

            };
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
                            if (!IsNullorEmptyString(ID) && (!IDs.Contains(ID)))
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
            return record => { return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"); };
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
                        CodeTypes.Add("CPC");
                    }
                }

                return new Dictionary<string, object[]>()
                {
                    {"code", Codes.ToArray()},
                    {"code_type", CodeTypes.ToArray()}
                };
            };
        }

        internal static Func<XElement, object> AbstractAccessor(DataSet sourceDataSet)
        {
            return record =>
            {
                var AbstractElements = record.Element("Abstracts").Elements("Abstract");
                var EnglishElements = AbstractElements.Where(el => el.Attribute("lang").Value == "en");
                var MachineTranslation = AbstractElements.Where(el => el.Attribute("lang").Value == "mt");
                var NonEnglishElements = AbstractElements.Where(el => (el.Attribute("lang").Value != "en") && (el.Attribute("lang").Value != "mt"));

                if (EnglishElements.Any())
                {
                    return EnglishElements.First().Value;
                }
                else if (NonEnglishElements.Count() == 1)
                {
                    var langCode = NonEnglishElements.First().Attribute("lang").Value;
                    var lutRow = sourceDataSet.Tables["language_lut"].Rows.Find(langCode);

                    if (lutRow != null)
                    {
                        if (lutRow["charset"].ToString() == "latin")
                        {
                            if (MachineTranslation.Any())
                            {
                                return NonEnglishElements.First().Value + " Translation: " + MachineTranslation.First().Value;
                            }
                            else
                            {
                                return NonEnglishElements.First().Value;
                            }
                        }
                        else
                        {
                            if (MachineTranslation.Any())
                            {
                                return MachineTranslation.First().Value;
                            }
                            else
                            {
                                return System.DBNull.Value;
                            }
                        }
                    }
                    else
                    {
                        throw new KeyDataNotFoundException($"language code {langCode} not found in languages_LUT", record,true);
                    }
                }
                else if (NonEnglishElements.Count() == 0)
                {
                    throw new KeyDataNotFoundException($"No abstract found", record, false);
                }
                else
                {
                    throw new KeyDataNotFoundException($"Multiple non-english abstracts found", record, true);
                }

            };
        }
    }
}
