﻿using System;
using System.Data;
using HybridDb.Config;
using HybridDb.Migrations;
using HybridDb.Migrations.Schema;

namespace HybridDb
{
    public interface IDocumentStore : IDisposable
    {
        Configuration Configuration { get; }
        StoreStats Stats { get; }

        bool IsInitialized { get; }
        bool Testing { get; }
        TableMode TableMode { get; }

        void Initialize();

        IDocumentSession OpenSession(DocumentTransaction tx = null);
        DocumentTransaction BeginTransaction(IsolationLevel level = IsolationLevel.ReadCommitted);
        DocumentTransaction BeginTransaction(Guid commitId, IsolationLevel level = IsolationLevel.ReadCommitted);

        void Execute(DdlCommand command);
        object Execute(DocumentTransaction tx, DmlCommand command);
        T Execute<T>(DocumentTransaction tx, Command<T> command);
    }
}