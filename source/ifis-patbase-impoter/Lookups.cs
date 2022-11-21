using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using Google.Protobuf.WellKnownTypes;
using MySql.Data.MySqlClient;
using System.Data;
using System.Diagnostics;

namespace ifis_patbase_importer
{
    public static class Lookups
    {
        public static DataSet AddCSVtoDataSet(this DataSet sourceDataSet, string tableName,  string strFilePath, string[] pkColumns)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            DataTable dt = new DataTable(tableName);
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(',');
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            sourceDataSet.Tables.Add(dt);
            //set pk
            var PrimaryKeyColumns = new System.Data.DataColumn[pkColumns.Length];
            foreach(string pkColumn in pkColumns)
            {
                PrimaryKeyColumns[Array.IndexOf(pkColumns,pkColumn)] = sourceDataSet.Tables[tableName].Columns[pkColumn];
            }
            sourceDataSet.Tables[tableName].PrimaryKey = PrimaryKeyColumns;
            stopwatch.Stop();
            Program.HandleMessage($"adding CSV {strFilePath} to source dataset took {stopwatch.ElapsedMilliseconds}ms");
            return sourceDataSet;
        }

        public static DataSet ReplaceInvalidPubIDs(this DataSet sourceDataset)
        {
            var IDReplacements = new Dictionary<int, int>
            {
                {1179,6660},
            };

            foreach (var id in IDReplacements.Keys)
            {
                var rows = sourceDataset.Tables["fsta"].Select("publication_ID='" + id + "'");
                foreach (var row in rows)
                    row["publication_id"]=IDReplacements[id];
            }

            return sourceDataset;
        }

    }
}
