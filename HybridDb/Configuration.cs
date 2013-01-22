﻿using System;
using System.Collections.Generic;
using HybridDb.Logging;
using HybridDb.Schema;

namespace HybridDb
{
    public class Configuration
    {
        readonly Dictionary<Type, ITable> tables;

        public Configuration()
        {
            tables = new Dictionary<Type, ITable>();
            Serializer = new DefaultBsonSerializer();
            Logger = new ConsoleLogger(LogLevel.Info, new LoggingColors());
        }

        public ILogger Logger { get; private set; }
        public ISerializer Serializer { get; private set; }

        public Dictionary<Type, ITable> Tables
        {
            get { return tables; }
        }

        public Table<TEntity> Register<TEntity>(string name)
        {
            var entity = new Table<TEntity>(name);
            tables.Add(typeof (TEntity), entity);
            return entity;
        }

        public void UseSerializer(ISerializer serializer)
        {
            Serializer = serializer;
        }

        public void UseSerializer(ILogger logger)
        {
            Logger = logger;
        }

        public ITable GetTableFor<T>()
        {
            return GetTableFor(typeof (T));
        }

        public ITable GetTableFor(Type type)
        {
            ITable value;
            if (tables.TryGetValue(type, out value))
                return value;

            throw new TableNotFoundException(type);
        }
    }
}