using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using HybridDb.Config;
using Newtonsoft.Json;

namespace HybridDb.Commands
{
    public abstract class DatabaseCommand
    {
        internal abstract PreparedDatabaseCommand Prepare(DocumentStore store, Guid etag, int uniqueParameterIdentifier);

        protected static IDictionary<Column, object> ConvertAnonymousToProjections(Table table, object projections)
        {
            return projections as IDictionary<Column, object> ??
                   (from projection in projections as IDictionary<string, object> ?? ObjectToDictionaryRegistry.Convert(projections)
                    let column = table[projection]
                    where column != null
                    select new KeyValuePair<Column, object>(column, projection.Value)).ToDictionary();
        }

        protected static Dictionary<string, Parameter> MapProjectionsToParameters(IDictionary<Column, object> projections, int i)
        {
            var parameters = new Dictionary<string, Parameter>();
            foreach (var projection in projections)
            {
                var column = projection.Key;
                var sqlColumn = SqlTypeMap.Convert(column);
                AddTo(parameters, "@" + column.Name + i, projection.Value, sqlColumn.DbType, sqlColumn.Length);
            }

            return parameters;
        }

        public static void AddTo(Dictionary<string, Parameter> parameters, string name, object value, DbType? dbType, int? size)
        {
            if (dbType == DbType.Xml)
            {
                var xml = JsonConvert.DeserializeXmlNode(JsonConvert.SerializeObject(new { item = value }), "root");

                using (var reader = new XmlNodeReader(xml))
                {
                    value = new SqlXml(reader);

                    Debug.WriteLine(((SqlXml) value).Value);
                }

                //var ms = new MemoryStream();
                //xml.Re
                //var writer = XmlWriter.Create(ms, new XmlWriterSettings { OmitXmlDeclaration = true });
                //new XmlSerializer(value.GetType()).Serialize(writer, value);
 
            }

            parameters[name] = new Parameter {Name = name, Value = value, DbType = dbType, Size = size};
        }

        public class PreparedDatabaseCommand
        {
            public string Sql { get; set; }
            public List<Parameter> Parameters { get; set; }
            public int ExpectedRowCount { get; set; }
        }
    }
}