using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Xml.Linq;
using MySql.Data.MySqlClient;

namespace ifis_patbase_importer
{
	public class Mapping
	{
		public Column[] TargetColumns;
		public Func<XElement, IDictionary<string, object[]>> DataAccessor;
		public Func<IDictionary<string, DataRow[]>, IDictionary<string, object[]>> FKDataAccessor;
		public Column ForeignKeyColumn;
		public bool AutoID;
		public Column SourceColumn;
		public bool IsPKMapping;


		//standard mapping
		public Mapping(Column targetColumn, Func<XElement, object> dataAccessor,
			flag flag = flag.None, Column foreignKeyColumn = null)
		{
			TargetColumns = new[] { targetColumn };
			DataAccessor = record => new Dictionary<string, object[]>() { { targetColumn.ColumnName, new[] { dataAccessor(record) } } };
			AutoID = (flag == flag.AutoID);
			ForeignKeyColumn = foreignKeyColumn;
		}

		//accessor returns multiple objects
		public Mapping(Column targetColumn, Func<XElement, object[]> dataAccessor) : this(targetColumn, dataAccessor,flag.None)
		{
			DataAccessor = record => new Dictionary<string, object[]>() { { targetColumn.ColumnName, dataAccessor(record) } };
        }

		//auto ID mapping
		public Mapping(Column targetColumn, flag flag) : this(targetColumn, null, flag) { }

		//foerign key mapping
		public Mapping(Column targetColumn, Column foreignKeyColumn) : this(targetColumn, null, flag.None, foreignKeyColumn) { }


		//foreign key mapping with foreign key accessor function
		public Mapping(Column[] targetColumns, Func<IDictionary<string, DataRow[]>, IDictionary<string, object[]>>
			fkDataAccessor, bool IsFKWithAccessorMapping) : this(null, null, flag.None, null)
		{
			TargetColumns = targetColumns;
			FKDataAccessor = fkDataAccessor;
		}

		// 1 accessor for multiple target columns mapping
		public Mapping(Column[] targetColumns, Func<XElement, IDictionary<string, object[]>> dataAccessor) :
			this(null, null, flag.None, null)
		{
			TargetColumns = targetColumns;
			DataAccessor = dataAccessor;
		}

		public enum flag
		{
			None,
			AutoID
		}
	}


	public class Column
	{
		public Table Table;
		public string ColumnName;
		public bool AutoID;
		public int Width;
		public Column(Table table, string columnName)
		{
			Table = table;
			ColumnName = columnName;
			AutoID = false;
			Width = 0;
		}

		public Column(Table table, string columnName, bool autoID)
		{
			Table = table;
			ColumnName = columnName;
			AutoID = autoID;
			Width = 0;
		}

	}

	public class Table
	{
		public string TableName;
		public string Database;
		public string[] PrimaryKeys;
		public Priority FillPriority;

		public Table(string table, string database, Priority fillPriority)
		{
			TableName = table;
			Database = database;
			FillPriority = fillPriority;
		}

		public Table(string table, string database, string[] primaryKeys)
		{
			TableName = table;
			Database = database;
			PrimaryKeys = primaryKeys;
		}

		public enum Priority
		{
			First = 1,
			Second = 2,
			Third = 3,
			Last = 4,
		}
	}
}