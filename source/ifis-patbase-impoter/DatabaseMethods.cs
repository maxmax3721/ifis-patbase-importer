using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.IO;
using System.Xml.Linq;
using System.Reflection.Emit;

namespace ifis_patbase_importer
{
    internal static class DatabaseMethods
    {
        public static DataSet AddToDataSet(this DataSet ds,Options opts, Table sourceTable, String whereStatement, String columns)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            //clear existing data if exists
            if (ds.Tables.Contains(sourceTable.TableName))
            {
                ds.Tables.Remove(sourceTable.TableName);
            }
            
            //db connection
            var cs = $"server={opts.server};userid={opts.userid};password={opts.password};database={sourceTable.Database};" +
            $"Convert Zero Datetime=True";
            var con = new MySqlConnection(cs);
            con.Open();

            //select data
            var mySqlDataAdapter = new MySqlDataAdapter($"SELECT {columns} from {sourceTable.TableName} "+whereStatement, con);
            mySqlDataAdapter.SelectCommand.CommandTimeout = 180;

            //populate datatable in dataset
            ds.Tables.Add(sourceTable.TableName);
            mySqlDataAdapter.Fill(ds.Tables[sourceTable.TableName]);
            
            con.Close();

            //define primary keys for new datatable
            var primaryKeyColums = new System.Data.DataColumn[sourceTable.PrimaryKeys.Count()];
            int i = 0;
            foreach (var primaryKey in sourceTable.PrimaryKeys)
            {
                primaryKeyColums[i++] = ds.Tables[sourceTable.TableName].Columns[primaryKey];
            }

            //add primary keys to source dataset
            ds.Tables[sourceTable.TableName].PrimaryKey = primaryKeyColums;

            stopwatch.Stop();
            Program.HandleMessage($"read source table {sourceTable.TableName} to dataset took " + stopwatch.ElapsedMilliseconds + "ms");
            return ds;

        }

        public static RecordLog MigrateData(Mapping[] mappings, XElement recordXElement, DataSet sourceDataset,Options opts)
        { 
            //target connection string (GC)
            var cs = $"server={opts.server};userid={opts.userid};password={opts.password};database={opts.targetDBName};" +
            $"Convert Zero Datetime=True";


            var con = new MySqlConnection(cs);
            con.Open();

            var myTrans = con.BeginTransaction();

            var targetDataset = new DataSet();
            var orderedTables = mappings.GroupBy(mapping => mapping.TargetColumns[0].Table).OrderBy(group => @group.Key.FillPriority).ToArray();

            //create array of data adapters and corresponding command builders
            var DataAdapters = new Dictionary<string, MySqlDataAdapter> { };
            var CommandBuilders = new Dictionary<string, MySqlCommandBuilder> { };
            foreach (Table table in mappings.Select(mapping => mapping.TargetColumns[0].Table).Distinct().ToArray())
            {
                DataAdapters.Add(table.TableName, new MySqlDataAdapter($"Select * from {table.TableName} limit 1", con));
                CommandBuilders.Add(table.TableName, new MySqlCommandBuilder(DataAdapters[table.TableName]));
                DataAdapters[table.TableName].FillSchema(targetDataset, SchemaType.Mapped, table.TableName);
            }

            //add temporary columns to pass data between tables
            targetDataset.Tables["authors"].Columns.Add(new DataColumn("temporary_author_type"));
            targetDataset.Tables["authors"].Columns.Add(new DataColumn("temporary_entry_no"));
            targetDataset.Tables["authors"].Columns.Add(new DataColumn("temporary_address"));
            

            //set auto increment seeds
            
            foreach (var mapping in mappings.Where(mapping => mapping.AutoID))
            {
                //query current auto increment value
                string autoIncrementQuery = $"SELECT AUTO_INCREMENT " +
                    $"FROM information_schema.TABLES " +
                    $"WHERE TABLE_SCHEMA = '{opts.targetDBName}' " +
                    $"AND TABLE_NAME = '{mapping.TargetColumns[0].Table.TableName}'";

                MySqlCommand cmd = new MySqlCommand(autoIncrementQuery, con);
                Program.HandleMessage(autoIncrementQuery);
                var AutoIncrement = cmd.ExecuteScalar();
                if (AutoIncrement == System.DBNull.Value)//first value
                    targetDataset.Tables[mapping.TargetColumns[0].Table.TableName].Columns[mapping.TargetColumns[0].ColumnName].AutoIncrementSeed = 1;
                else
                    targetDataset.Tables[mapping.TargetColumns[0].Table.TableName].Columns[mapping.TargetColumns[0].ColumnName].AutoIncrementSeed = Convert.ToInt32(AutoIncrement);

            }

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var newRowsForRecord = new Dictionary<string, DataRow[]>();
            string tableName = string.Empty;

            foreach (var tableMappings in orderedTables)
            {
                tableName = tableMappings.Key.TableName;
                var newRows = GetTableDataForAbstract(targetDataset.Tables[tableName], tableMappings.ToArray(), recordXElement, newRowsForRecord);
                newRowsForRecord[tableName] = newRows;
            }

            stopwatch.Stop();
            Program.HandleMessage($"Data Proccessing took {stopwatch.ElapsedMilliseconds}ms");
            stopwatch.Restart();

            //write to grand_central
            foreach (string table in orderedTables.Select(mapping => mapping.Key.TableName).Distinct().ToArray())
            {
                DataAdapters[table].Update(targetDataset, table);
            }

            myTrans.Commit();

            stopwatch.Stop();
            Program.HandleMessage($"Write tables to {opts.targetDBName} {stopwatch.ElapsedMilliseconds}ms");
            con.Close();

            //try record info
            var label = targetDataset.Tables["fsta"].Rows[0]["label"].ToString();
            var pubNum = targetDataset.Tables["fsta"].Rows[0]["patent_number"].ToString().Split("")[0];
            return new RecordLog { Label = label, PublicationNumber = pubNum };
            
        }

        private static DataRow[] GetTableDataForAbstract(DataTable dataTable, Mapping[] mappings,XElement article, IDictionary<string, DataRow[]> newRowsForAbstract)
        {
            var firstColumnInTable = true;
            DataRow[] newRows = null;
            foreach (var mapping in mappings.Where(mapping => !(mapping.AutoID)))
            {
                IDictionary<string,object[]> columnsData = 
                    mapping.ForeignKeyColumn != null
                    ? new Dictionary<string, object[]> { { mapping.TargetColumns[0].ColumnName, newRowsForAbstract[mapping.ForeignKeyColumn.Table.TableName].Select(row => row[mapping.ForeignKeyColumn.ColumnName]).ToArray() } }
                    : mapping.FKDataAccessor != null 
                        ? mapping.FKDataAccessor(newRowsForAbstract)
                        : mapping.DataAccessor(article);

                //handle return of no data for mapping
                if (columnsData == null)
                    break;

                foreach (var columnData in columnsData)
                {
                    if (firstColumnInTable)
                    {
                        newRows = columnData.Value.Select(_ => dataTable.NewRow()).ToArray();
                        firstColumnInTable = false;
                    }

                    foreach (var rowData in newRows.Select((row,index) => new {row,index}))
                    {
                        rowData.row[columnData.Key] = columnData.Value.Length > 1 ? columnData.Value[rowData.index] : columnData.Value[0];
                    }
                }
                
            }

            //handle return of no data for table
            if (newRows == null)
                return newRows;

            foreach (var row in newRows)
            {
                try
                {
                    dataTable.Rows.Add(row);
                }
                catch (System.Data.NoNullAllowedException e)
                {
                    Program.WriteError(Console.Error, "", e.GetType().FullName, e.Message);
                }
                
            }

            return newRows;
        }

        public static int GetMaxPbLabelInt(Options opts)
        {
            var cs = $"server={opts.server};userid={opts.userid};password={opts.password};database={opts.targetDBName};" +
            $"Convert Zero Datetime=True";

            var con = new MySqlConnection(cs);
            con.Open();

            //query current auto increment value
            string autoIncrementQuery = $"SELECT MAX(label)" +
                $"FROM fsta " +
                $"WHERE label like 'PB%'";

            MySqlCommand cmd = new MySqlCommand(autoIncrementQuery, con);
            Program.HandleMessage(autoIncrementQuery);
            var maxLabel = cmd.ExecuteScalar();

            con.Close();

            if(maxLabel != System.DBNull.Value)
            {
                return int.Parse(maxLabel.ToString().Replace("PB", ""));
            }
            else
            {
                return 0;
            }
        }
    }
}
