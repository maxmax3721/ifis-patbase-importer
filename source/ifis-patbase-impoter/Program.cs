using CommandLine;
using CsvHelper;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks.Sources;
using System.Xml.Linq;

namespace ifis_patbase_importer
{
    internal class Program
    {
        static Options options;

        public static void HandleMessage(string message)
        {
            if (options.verbose)
            {
                Console.WriteLine(message);
            }
        }

        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args)
            .WithParsed<Options>(opts =>
            {

                options = opts;

                //define target tables and fill priority
                var record_source = new Table("record_source", opts.targetDBName, Table.Priority.First);
                var authors = new Table("authors", opts.targetDBName, Table.Priority.First);
                var publishers = new Table("publishers", opts.targetDBName, Table.Priority.First);
                var thesaurus = new Table("thesaurus", opts.targetDBName, Table.Priority.First);
                var journals = new Table("journals", opts.targetDBName, Table.Priority.Second);
                var fsta = new Table("fsta", opts.targetDBName, Table.Priority.Third);
                var fsta_authors_join = new Table("fsta_authors_join", opts.targetDBName, Table.Priority.Last);
                var addresses = new Table("addresses", opts.targetDBName, Table.Priority.Last);
                var languages = new Table("languages", opts.targetDBName, Table.Priority.Last);
                var commercial_names = new Table("commercial_names", opts.targetDBName, Table.Priority.Last);
                var controlled_major = new Table("controlled_major", opts.targetDBName, Table.Priority.Last);
                var classification_codes = new Table("classification_codes", opts.targetDBName, Table.Priority.Last);

                
                var sourceDataSet = new DataSet();

                //add gc journals table to source dataset
                var journalsSource = new Table("journals", opts.targetDBName, new[] { "journal_id" });
                sourceDataSet.AddToDataSet(opts, journalsSource,null,"*");

                //add LUTs to dataset
                sourceDataSet.AddCSVtoDataSet("journal_id_lut", "config/LookupCSVs/JournalIDLUT.csv", new[] { "CountryCode", "KindCode" });
                sourceDataSet.AddCSVtoDataSet("controlled_major_lut", "config/LookupCSVs/ControlledMajorLUT.csv", new[] { "Patent_code" });
                sourceDataSet.AddCSVtoDataSet("language_lut", "config/LookupCSVs/LanguagesLUT.csv", new[] { "patbase_language_code" });
                sourceDataSet.AddCSVtoDataSet("kind_code_lut", "config/LookupCSVs/GCKindCodeLUT.csv", new[] { "CountryCode", "KindCode" });

                //Define record-wise mappings
                var Mappings = new[]
                {
                    //fsta
                    new Mapping(new[] {
                        new Column(fsta,"label"),
                        new Column(fsta,"accession_number") 
                    }, DataAccess.LabelAccessor(opts)),
                    new Mapping(new Column(fsta,"document_type"), record => {return "Patent"; }),
                    new Mapping(new[] {
                        new Column(fsta,"publication_title"),
                        new Column(fsta,"journal_id"),
                        new Column(fsta,"publisher_id")
                    }, DataAccess.PublicationAccessor(sourceDataSet)),
                    new Mapping(new Column(fsta,"abstract"),DataAccess.AbstractAccessor(sourceDataSet)),
                    new Mapping(new Column [] {
                        new Column(fsta,"english_title"),
                        new Column(fsta,"foreign_title") 
                    }, DataAccess.TitleAccessor(sourceDataSet)),
                    new Mapping(new Column(fsta,"patent_number"), DataAccess.PatentNumAccessor(sourceDataSet)),
                    new Mapping(new Column(fsta,"patent_priority"), DataAccess.PatentPriorityAccessor()),
                    new Mapping(new Column(fsta,"section"), record => {return "V"; }),
                    new Mapping(new Column(fsta,"subsection"), record => {return System.DBNull.Value; }),
                    new Mapping(new Column(fsta,"included_status"), record => {return 0; }),
                    new Mapping(new Column(fsta,"date_addition"), DataAccess.GetDate()),
                    new Mapping(new Column(fsta,"date_received"), DataAccess.GetDate()),
                    new Mapping(new []{
                        new Column(fsta,"date_published"),
                        new Column(fsta,"publication_date_year"),
                        new Column(fsta,"publication_date_month"),
                        new Column(fsta,"publication_date_day")
                    }, DataAccess.PublicationDateFieldAccessor()),
                    new Mapping(new Column(fsta,"source_id"), record => { return 3; }),

                    //fsta_authors_join
                    new Mapping(new Column(fsta_authors_join,"author_type"),new Column(authors,"TEMPORARY_author_type")),
                    new Mapping(new Column(fsta_authors_join,"entry_no"),new Column(authors,"TEMPORARY_entry_no")),
                    new Mapping(new Column(fsta_authors_join,"author_ID"),new Column(authors,"author_ID")),
                    new Mapping(new Column(fsta_authors_join,"label"),new Column(fsta,"label")),

                    //authors
                    new Mapping(new Column(authors,"author_ID"), Mapping.flag.AutoID),
                    new Mapping(new [] {
                        new Column(authors,"author_name"),
                        new Column(authors,"TEMPORARY_author_type"),
                        new Column(authors,"TEMPORARY_entry_no"),
                        new Column(authors,"TEMPORARY_address")
                    }, DataAccess.AuthorsAccessor(sourceDataSet)),

                    //addresses
                    new Mapping(new Column[]{
                        new Column(addresses,"author_ID"),
                        new Column(addresses,"address")
                    }, DataAccess.AuthorIDAccessor(sourceDataSet),true),
                    
                    //languages
                    new Mapping(new Column[]{
                        new Column(languages,"entry_no"),
                        new Column(languages,"summary_source"),
                        new Column(languages,"language_code"),
                        new Column(languages,"language")
                    }, DataAccess.LanguageAccessor(sourceDataSet)),
                    new Mapping(new Column(languages,"label"),new Column(fsta,"label")),

                    //controlled_major
                    new Mapping(new Column(controlled_major,"thesaurus_id"), DataAccess.ThesaurusIDAccessor(sourceDataSet)),
                    new Mapping(new Column(controlled_major,"inserted_date"),DataAccess.GetDate()),
                    new Mapping(new Column(controlled_major,"label"),new Column(fsta,"label")),

                    //classification_codes
                    new Mapping(new[] {
                        new Column(classification_codes,"code"),
                        new Column(classification_codes,"code_type")
                    }, DataAccess.ClassificationCodeAccessor()),
                    new Mapping(new Column(classification_codes,"label"),new Column(fsta,"label"))

                };

                var recordsMigrated = 0;
                var recordsFailed = 0;

                if (File.Exists("log.csv"))
                {
                    System.IO.File.WriteAllText("log.csv", string.Empty);
                }
                var logger = new CsvLogger("log.csv");
                logger.csvWriter.WriteHeader<RecordLog>();
                logger.csvWriter.NextRecord();

                foreach (var sourceFilePath in opts.xmlPath)
                {
                    XDocument sourceXDoc = XDocument.Load(sourceFilePath);

                    foreach (XElement RecordXElement in sourceXDoc.Root.Elements("Family").Elements("Patent"))
                    {
                        try
                        {
                            //migrate data
                            var recordLog = DatabaseMethods.MigrateData(Mappings, RecordXElement, sourceDataSet, opts);
                            //log record migration
                            recordsMigrated = recordsMigrated + 1;
                            logger.csvWriter.WriteRecord(recordLog);
                            logger.csvWriter.NextRecord();
                            logger.csvWriter.Flush();
                        }
                        catch (ifis_patbase_importer.KeyDataNotFoundException e)
                        {
                            if (e.Log)
                            {
                                Program.WriteDataNotFoundError(Console.Error, e.Record, e.Message);
                            }
                            recordsFailed = recordsFailed + 1;
                        }
                        catch (MySql.Data.MySqlClient.MySqlException e)
                        {
                            Program.WriteError(Console.Error, "Transaction Failed", e.GetType().ToString(), e.Message);
                            Program.WriteError(Console.Error, $"", "Data for record were not migrated", "");
                            recordsFailed = recordsFailed + 1;
                        }


                    }
                }
                Program.HandleMessage($"Migration complete. {recordsMigrated+recordsFailed} records processed. {recordsMigrated} records migrated. {recordsFailed} records failed.");
            });
        }

        public static void WriteError(TextWriter output, string id, string exceptionType, object message)
        {
            output.WriteLine($"{id}: {exceptionType}: {message}");
            output.Flush();
        }

        public static void WriteDataNotFoundError(TextWriter output, XElement record, string message)
        {

            var countryCode = record.Element("CountryCode") != null ? record.Element("CountryCode").Value : null;
            var kindCode = record.Element("KindCode") != null ? record.Element("KindCode").Value : null;
            var patNum = record.Attribute("pn") != null ? record.Attribute("pn").Value : null;
            output.WriteLine($"{patNum}: {countryCode}: {kindCode}: {message}");
            output.Flush();
        }

        public static void ExecuteCommands(string[] Commands, Options opts, string dbname)
        {
            var cs = $"server={opts.server};userid={opts.userid};password={opts.password};database={dbname};" +
                    $"Convert Zero Datetime=True";
            var con = new MySqlConnection(cs);
            con.Open();

            foreach (string commandString in Commands)
            {
                MySqlCommand cmd = new MySqlCommand(commandString, con);
                cmd.CommandTimeout = 300;

                Program.HandleMessage(commandString);

                try
                {
                    int rowsAffected = cmd.ExecuteNonQuery();
                    Program.HandleMessage($"{rowsAffected} rows affected");
                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Program.HandleMessage(ex.Message);
                }
            }
            con.Close();
        }
    }
}
