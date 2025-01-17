﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HybridDb.Commands;
using HybridDb.Config;
using HybridDb.Events;
using HybridDb.Events.Commands;
using HybridDb.Linq.Old;
using HybridDb.Migrations.Documents;

namespace HybridDb
{
    public class DocumentSession : IDocumentSession, IAdvancedDocumentSession
    {
        readonly IDocumentStore store;

        readonly ManagedEntities entities = new();
        readonly List<(int Generation, EventData<byte[]> Data)> events;
        readonly List<DmlCommand> deferredCommands;
        readonly DocumentMigrator migrator;

        DocumentTransaction enlistedTx;

        bool saving = false;

        internal DocumentSession(IDocumentStore store, DocumentMigrator migrator, DocumentTransaction tx = null)
        {
            deferredCommands = new List<DmlCommand>();
            events = new List<(int Generation, EventData<byte[]> Data)>();

            this.migrator = migrator;
            this.store = store;

            Enlist(tx);
        }

        public IDocumentStore DocumentStore => store;
        public DocumentTransaction DocumentTransaction => enlistedTx;
        public IReadOnlyList<DmlCommand> DeferredCommands => deferredCommands;

        public IAdvancedDocumentSession Advanced => this;
        public IReadOnlyDictionary<EntityKey, ManagedEntity> ManagedEntities => entities;

        public T Load<T>(string key) where T : class => (T)Load(typeof(T), key);

        public object Load(Type type, string key)
        {
            var design = store.Configuration.GetOrCreateDesignFor(type);

            if (entities.TryGetValue(new EntityKey(design.Table, key), out var managedEntity))
            {
                if (managedEntity.State == EntityState.Deleted) return null;

                var entityType = managedEntity.Entity.GetType();
                if (!type.IsAssignableFrom(entityType))
                {
                    throw new InvalidOperationException($"Document with id '{key}' exists, but is of type '{entityType}', which is not assignable to '{type}'.");
                }

                return managedEntity.Entity;
            }

            var row = Transactionally(tx => tx.Get(design.Table, key));
            
            if (row == null) return null;

            var concreteDesign = store.Configuration.GetOrCreateDesignByDiscriminator(design, (string)row[DocumentTable.DiscriminatorColumn]);

            // The discriminator does not map to a type that is assignable to the expected type.
            if (!type.IsAssignableFrom(concreteDesign.DocumentType))
            {
                throw new InvalidOperationException($"Document with id '{key}' exists, but is of type '{concreteDesign.DocumentType}', which is not assignable to '{type}'.");
            }

            return ConvertToEntityAndPutUnderManagement(concreteDesign, row);
        }


        /// <summary>
        /// Query for document of type T and subtypes of T in the table assigned to T.
        /// Note that if a subtype of T is specifically assigned to another table in store configuration,
        /// it will not be included in the result of Query&lt;T&gt;().
        /// </summary>
        public IQueryable<T> Query<T>() where T : class
        {
            var configuration = store.Configuration;

            var design = configuration.GetOrCreateDesignFor(typeof(T));

            var include = design.DecendentsAndSelf.Keys.ToArray();
            var exclude = design.Root.DecendentsAndSelf.Keys.Except(include).ToArray();

            // Include T and known subtybes, exclude known supertypes.
            // Unknown discriminators will be included and filtered result-side
            // but also be added to configuration so they are known on next query.
            var query = new Query<T>(new QueryProvider(this, design)).Where(x =>
                x.Column<string>("Discriminator").In(include)
                || !x.Column<string>("Discriminator").In(exclude));

            return query;
        }

        public void Defer(DmlCommand command)
        {
            deferredCommands.Add(command);
        }

        public void Evict(object entity)
        {
            var managedEntity = TryGetManagedEntity(entity);

            if (managedEntity == null) return;

            entities.Remove(managedEntity.EntityKey);
        }

        public Guid? GetEtagFor(object entity) => TryGetManagedEntity(entity)?.Etag;
        
        public bool Exists<T>(string key, out Guid? etag) where T : class => Exists(typeof(T), key, out etag);

        public bool Exists(Type type, string key, out Guid? etag)
        {
            if (TryGetManagedEntity(type, key, out var entity))
            {
                etag = entity.Etag;
                return true;
            }

            var design = store.Configuration.GetOrCreateDesignFor(type);

            etag = Transactionally(tx => tx.Execute(new ExistsCommand(design.Table, key)));

            return etag != null;
        }

        public Dictionary<string, List<string>> GetMetadataFor(object entity) => TryGetManagedEntity(entity)?.Metadata;

        public void SetMetadataFor(object entity, Dictionary<string, List<string>> metadata)
        {
            var managedEntity = TryGetManagedEntity(entity);
            
            if (managedEntity == null) return;

            managedEntity.Metadata = metadata;
        }

        public void Store(object entity) => Store(null, entity);
        public void Store(object entity, Guid? etag) => Store(null, entity, etag);
        public void Store(string key, object entity, Guid? etag) => Store(key, entity, etag, EntityState.Loaded);
        public void Store(string key, object entity) => Store(key, entity, null, EntityState.Transient);

        void Store(string key, object entity, Guid? etag, EntityState state)
        {
            if (entity == null) return;

            var design = store.Configuration.GetOrCreateDesignFor(entity.GetType());

            key ??= design.GetKey(entity);

            var entityKey = new EntityKey(design.Table, key);

            if (entities.TryGetValue(entityKey, out var managedEntity) || 
                entities.TryGetValue(entity, out managedEntity))
            {
                // Storing a new instance under an existing id, is an error
                if (managedEntity.Entity != entity)
                    throw new HybridDbException($"Attempted to store a different object with id '{key}'.");

                // Storing a same instance under an new id, is an error
                // Table cannot change as it's tied to entity's type
                if (!Equals(managedEntity.EntityKey, entityKey))
                    throw new HybridDbException($"Attempted to store same object '{managedEntity.Key}' with a different id '{key}'. Did you forget to evict?");

                // Storing same instance is a noop
                return;
            }

            entities.Add(new ManagedEntity(entityKey)
            {
                Entity = entity,
                State = state,
                Etag = etag
            });
        }

        public void Append(int generation, EventData<byte[]> @event) => events.Add((generation, @event));

        public void Delete(object entity)
        {
            var managedEntity = TryGetManagedEntity(entity);
            if (managedEntity == null) return;

            if (managedEntity.State == EntityState.Transient)
            {
                entities.Remove(managedEntity.EntityKey);
            }
            else
            {
                managedEntity.State = EntityState.Deleted;
            }
        }

        public Guid SaveChanges() => SaveChanges(lastWriteWins: false, forceWriteUnchangedDocument: false);

        public Guid SaveChanges(bool lastWriteWins, bool forceWriteUnchangedDocument)
        {
            if (saving)
            {
                throw new InvalidOperationException("Session is not in a valid state. Please dispose it and open a new one.");
            }

            saving = true;

            var commands = new Dictionary<ManagedEntity, DmlCommand>();
            foreach (var managedEntity in entities.Values.ToList())
            {
                var key = managedEntity.Key;
                var design = store.Configuration.GetExactDesignFor(managedEntity.Entity.GetType());
                var projections = design.Projections.ToDictionary(x => x.Key, x => x.Value.Projector(managedEntity.Entity, managedEntity.Metadata));

                var configuredVersion = (int)projections[DocumentTable.VersionColumn];
                var document = (string)projections[DocumentTable.DocumentColumn];
                var metadataDocument = (string)projections[DocumentTable.MetadataColumn];

                var expectedEtag = !lastWriteWins ? managedEntity.Etag : null;

                switch (managedEntity.State)
                {
                    case EntityState.Transient:
                        commands.Add(managedEntity, new InsertCommand(design.Table, key, projections));
                        managedEntity.State = EntityState.Loaded;
                        managedEntity.Version = configuredVersion;
                        managedEntity.Document = document;
                        break;
                    case EntityState.Loaded:
                        if (!forceWriteUnchangedDocument && 
                            SafeSequenceEqual(managedEntity.Document, document) && 
                            SafeSequenceEqual(managedEntity.MetadataDocument, metadataDocument))
                            break;

                        commands.Add(managedEntity, new UpdateCommand(design.Table, key, expectedEtag, projections));

                        if (configuredVersion != managedEntity.Version && !string.IsNullOrEmpty(managedEntity.Document))
                        {
                            store.Configuration.BackupWriter.Write(
                                $"{design.DocumentType.FullName}_{key}_{managedEntity.Version}.bak", 
                                Encoding.UTF8.GetBytes(managedEntity.Document));
                        }

                        managedEntity.Version = configuredVersion;
                        managedEntity.Document = document;

                        break;
                    case EntityState.Deleted:
                        commands.Add(managedEntity, new DeleteCommand(design.Table, key, expectedEtag));
                        entities.Remove(new EntityKey(design.Table, managedEntity.Key));
                        break;
                }
            }

            var commitId = Transactionally(resultingTx =>
            {
                foreach (var command in commands.Select(x => x.Value).Concat(deferredCommands))
                {
                    store.Execute(resultingTx, command);
                }

                if (store.Configuration.EventStore)
                {
                    var eventTable = store.Configuration.Tables.Values.OfType<EventTable>().Single();

                    foreach (var @event in events)
                    {
                        resultingTx.Execute(new AppendEvent(eventTable, @event.Generation, @event.Data));
                    }
                }

                return resultingTx.CommitId;
            });

            foreach (var managedEntity in commands.Keys)
            {
                managedEntity.Etag = commitId;
            }

            saving = false;

            return commitId;
        }

        public void Dispose() {}

        internal object ConvertToEntityAndPutUnderManagement(DocumentDesign concreteDesign, IDictionary<string, object> row)
        {
            var key = (string)row[DocumentTable.IdColumn];

            var entityKey = new EntityKey(concreteDesign.Table, key);

            if (entities.TryGetValue(entityKey, out var managedEntity))
            {
                if (managedEntity.State == EntityState.Deleted) return null;

                return managedEntity.Entity;
            }

            var document = (string)row[DocumentTable.DocumentColumn];
            var documentVersion = (int) row[DocumentTable.VersionColumn];

            var entity = migrator.DeserializeAndMigrate(this, concreteDesign, row);

            if (entity.GetType() != concreteDesign.DocumentType)
            {
                throw new InvalidOperationException($"Requested a document of type '{concreteDesign.DocumentType}', but got a '{entity.GetType()}'.");
            }

            var metadataDocument = (string)row[DocumentTable.MetadataColumn];
            var metadata = metadataDocument != null
                ? (Dictionary<string, List<string>>) store.Configuration.Serializer.Deserialize(metadataDocument, typeof(Dictionary<string, List<string>>)) 
                : null;

            managedEntity = new ManagedEntity(entityKey)
            {
                Entity = entity,
                Document = document,
                Metadata = metadata,
                MetadataDocument = metadataDocument,
                Etag = (Guid) row[DocumentTable.EtagColumn],
                Version = documentVersion,
                State = EntityState.Loaded
            };

            entities.Add(managedEntity);
            return entity;
        }

        internal T Transactionally<T>(Func<DocumentTransaction, T> func) =>
            enlistedTx != null 
                ? func(enlistedTx) 
                : store.Transactionally(func);

        public void Clear()
        {
            entities.Clear();
            deferredCommands.Clear();
            enlistedTx = null;
        }

        public bool TryGetManagedEntity<T>(string key, out T entity)
        {
            if (TryGetManagedEntity(typeof(T), key, out var entityObject))
            {
                entity = (T) entityObject.Entity;
                return true;
            }

            entity = default;
            return false;
        }

        public bool TryGetManagedEntity(Type type, string key, out ManagedEntity entity) => 
            entities.TryGetValue(new EntityKey(store.Configuration.GetOrCreateDesignFor(type).Table, key), out entity);

        public void Enlist(DocumentTransaction tx)
        {
            if (tx == null)
            {
                enlistedTx = null;
                return;
            }

            if (!ReferenceEquals(tx.Store, DocumentStore))
            {
                throw new ArgumentException("Cannot enlist in a transaction that does not originate from the same store as the session.");
            }

            enlistedTx = tx;
        }

        ManagedEntity TryGetManagedEntity(object entity) => 
            entities.TryGetValue(entity, out var managedEntity) 
                ? managedEntity 
                : null;

        bool SafeSequenceEqual<T>(IEnumerable<T> first, IEnumerable<T> second)
        {
            if (Equals(first, second))
                return true;

            if (first == null || second == null)
                return false;

            return first.SequenceEqual(second);
        }
    }
}