using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace HybridDb.Schema
{
    public class DocumentDesign
    {
        public DocumentDesign(Configuration configuration, IndexTable indexTable, DocumentTable table, Type type)
        {
            Configuration = configuration;
            Type = type;
            Table = table;
            Projections = new Dictionary<string, Func<object, object>>();
        }

        public Type Type { get; private set; }
        public DocumentTable Table { get; private set; }
        public Dictionary<string, Func<object, object>> Projections { get; private set; }
        public Configuration Configuration { get; private set; }

        public void MigrateSchema()
        {
            Configuration.Store.Migrate(migrator => migrator.MigrateTo(Table));
        }

        protected Func<object, object> Compile<TEntity, TMember>(string name, Expression<Func<TEntity, TMember>> projector)
        {
            var compiled = projector.Compile();
            return entity =>
            {
                try
                {
                    return (object) compiled((TEntity) entity);
                }
                catch (Exception ex)
                {
                    throw new TargetInvocationException(
                        string.Format("The projector for column {0} threw an exception.\nThe projector code is {1}.", name, projector), ex);
                }
            };
        }
    }

    public class DocumentDesign<TEntity> : DocumentDesign
    {
        public DocumentDesign(Configuration configuration, IndexTable indexTable, DocumentTable table) : base(configuration, indexTable, table, typeof (TEntity)) {}

        public DocumentDesign<TEntity> Project<TMember>(Expression<Func<TEntity, TMember>> projector, bool makeNullSafe = true)
        {
            var name = Configuration.GetColumnNameByConventionFor(projector);
            return Project(name, projector, makeNullSafe);
        }

        public DocumentDesign<TEntity> Project<TMember>(string name, Expression<Func<TEntity, TMember>> projector, bool makeNullSafe = true)
        {
            if (makeNullSafe)
            {
                var nullCheckInjector = new NullCheckInjector();
                var nullCheckedProjector = (Expression<Func<TEntity, object>>) nullCheckInjector.Visit(projector);

                var column = new Column(name, new SqlColumn(typeof (TMember)))
                {
                    SqlColumn =
                    {
                        Nullable = !nullCheckInjector.CanBeTrustedToNeverReturnNull
                    }
                };

                Table.Register(column);
                Projections.Add(column, Compile(name, nullCheckedProjector));
            }
            else
            {
                var column = new Column(name, new SqlColumn(typeof (TMember)));
                Table.Register(column);
                Projections.Add(column, Compile(name, projector));
            }

            return this;
        }

        public DocumentDesign<TEntity> Project<TMember>(Expression<Func<TEntity, IEnumerable<TMember>>> projector, bool makeNullSafe = true)
        {
            return this;
        }
    }
}