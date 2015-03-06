﻿using System;
using System.Collections.Generic;
using System.Linq;
using HybridDb.Config;

namespace HybridDb
{
    public class Schema : ISchema
    {
        readonly DocumentStore store;
        readonly TableMode tableMode;

        public Schema(DocumentStore store, TableMode tableMode)
        {
            this.store = store;
            this.tableMode = tableMode;
        }

//        public bool TableExists(string name)
//        {
//            if (tableMode == TableMode.UseRealTables)
//            {
//                return store.RawQuery<dynamic>(string.Format("select OBJECT_ID('{0}') as Result", name)).First().Result != null;
//            }
        
//            return store.RawQuery<dynamic>(string.Format("select OBJECT_ID('tempdb..{0}') as Result", store.FormatTableName(name))).First().Result != null;
//        }

//        public List<string> GetTables()
//        {
//            return tableMode == TableMode.UseRealTables
//                ? store.RawQuery<string>("select table_name from information_schema.tables where table_type='BASE TABLE'").ToList()
//                : store.RawQuery<string>("select * from tempdb.sys.objects where object_id('tempdb.dbo.' + name, 'U') is not null AND name LIKE '#%'")
//                    .ToList();
//        }

//        public Column GetColumn(string table, string column)
//        {
//            if (tableMode == TableMode.UseRealTables)
//            {
//                var c = store.RawQuery<Column2>(
//                    string.Format(
//                        "select * from sys.columns where Name = N'{0}' and Object_ID = Object_ID(N'{1}')", column,
//                        table)).FirstOrDefault();

//                throw new Exception();
//            }

//            throw new Exception();

//            store.RawQuery<Column2>(
//                    string.Format(
//                        "select * from tempdb.sys.columns where Name = N'{0}' and Object_ID = Object_ID(N'tempdb..{1}')",
//                        column, store.FormatTableName(table))).FirstOrDefault();
//        }

//        public string GetType(int id)
//        {
//            var rawQuery = store.RawQuery<string>("select name from sys.types where system_type_id = @id", new {id});
//            return rawQuery.FirstOrDefault();
//        }

//        public bool IsPrimaryKey(string column)
//        {
//            var sql =
//                @"SELECT K.TABLE_NAME,
//                  K.COLUMN_NAME,
//                  K.CONSTRAINT_NAME
//                  FROM tempdb.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C
//                  JOIN tempdb.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K
//                  ON C.TABLE_NAME = K.TABLE_NAME
//                  AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG
//                  AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA
//                  AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME
//                  WHERE C.CONSTRAINT_TYPE = 'PRIMARY KEY'
//                  AND K.COLUMN_NAME = '" + column + "'";

//            var isPrimaryKey = store.RawQuery<dynamic>(sql).Any();
//            return isPrimaryKey;
//        }

        class TempColumn
        {
            public string Name { get; set; }
            public int system_type_id { get; set; }
            public int max_length { get; set; }
        }

        public Dictionary<string, Table> GetSchema()
        {
            return new Dictionary<string, Table>();
            var schema = new Dictionary<string, Table>();
            if (tableMode == TableMode.UseRealTables)
            {
                var realTables = store.RawQuery<string>("select table_name from information_schema.tables where table_type='BASE TABLE'").ToList();
                foreach (var tableName in realTables)
                {
                    var columns = store.RawQuery<TempColumn>(
                        string.Format("select * where Object_ID = Object_ID(N'{0}')", tableName));
                    //schema.Add(tableName, new Table(tableName, columns));
                }
            
                throw new Exception();
            }

            var tempTables = store.RawQuery<string>("select * from tempdb.sys.objects where object_id('tempdb.dbo.' + name, 'U') is not null AND name LIKE '#%'");
            foreach (var tableName in tempTables)
            {
                var formattedTableName = tableName.Remove(tableName.Length - 12, 12).TrimEnd('_');

                var columns = store.RawQuery<TempColumn>(
                    string.Format("select * from tempdb.sys.columns where Object_ID = Object_ID(N'tempdb..{0}')", tableName));
                
                schema.Add(tableName, new Table(tableName, columns.Select(Map)));

            }
            return schema;
        }

        Column Map(TempColumn column)
        {
            return new Column(column.Name, GetType(column.system_type_id));
        }

        Type GetType(int id)
        {
            var rawQuery = store.RawQuery<string>("select name from sys.types where system_type_id = @id", new { id });
            var typeAsString = rawQuery.FirstOrDefault();


            //"SELECT schemas.name AS [Schema], " +
            //"tables.name AS [Table], " +
            //"columns.name AS [Column], CASE WHEN columns.system_type_id = 34 THEN 'byte[]' WHEN columns.system_type_id = 35 THEN 'string' WHEN columns.system_type_id = 36 THEN 'System.Guid' WHEN columns.system_type_id = 48 THEN 'byte' WHEN columns.system_type_id = 52 THEN 'short' WHEN columns.system_type_id = 56 THEN 'int' WHEN columns.system_type_id = 58 THEN 'System.DateTime' WHEN columns.system_type_id = 59 THEN 'float' WHEN columns.system_type_id = 60 THEN 'decimal' WHEN columns.system_type_id = 61 THEN 'System.DateTime' WHEN columns.system_type_id = 62 THEN 'double' WHEN columns.system_type_id = 98 THEN 'object' WHEN columns.system_type_id = 99 THEN 'string' WHEN columns.system_type_id = 104 THEN 'bool' WHEN columns.system_type_id = 106 THEN 'decimal' WHEN columns.system_type_id = 108 THEN 'decimal' WHEN columns.system_type_id = 122 THEN 'decimal' WHEN columns.system_type_id = 127 THEN 'long' WHEN columns.system_type_id = 165 THEN 'byte[]' WHEN columns.system_type_id = 167 THEN 'string' WHEN columns.system_type_id = 173 THEN 'byte[]' WHEN columns.system_type_id = 175 THEN 'string' WHEN columns.system_type_id = 189 THEN 'long' WHEN columns.system_type_id = 231 THEN 'string' WHEN columns.system_type_id = 239 THEN 'string' WHEN columns.system_type_id = 241 THEN 'string' WHEN columns.system_type_id = 241 THEN 'string' END AS [Type], columns.is_nullable AS [Nullable]FROM sys.tables tables INNER JOIN sys.schemas schemas ON (tables.schema_id = schemas.schema_id ) INNER JOIN sys.columns columns ON (columns.object_id = tables.object_id) WHERE tables.name <> 'sysdiagrams' AND tables.name <> 'dtproperties' ORDER BY [Schema], [Table], [Column], [Type]""
            
            switch (typeAsString)
            {
                case "int":
                    return typeof (int);
                default:
                    throw new ArgumentOutOfRangeException("id");

            }

            return null;
        }
    }
}