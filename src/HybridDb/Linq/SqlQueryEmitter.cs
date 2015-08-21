using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using HybridDb.Config;
using HybridDb.Linq.Builders;

namespace HybridDb.Linq
{
    public class SqlQueryEmitter
    {
        public SqlQuery Translate(Configuration configuration, Expression expression)
        {
            var queryBuilder = new QueryBuilder(configuration);
            
            // parameters are indexed by value to ease reusing params by value
            var parameters = new Dictionary<object, string>();

            queryBuilder.Visit(expression);

            var selectSql = new StringBuilder();
            if (queryBuilder.Select != null)
                new SqlCodeGenerator(selectSql, parameters).Visit(queryBuilder.Select);

            var whereSql = new StringBuilder();
            if (queryBuilder.Where != null)
                new SqlCodeGenerator(whereSql, parameters).Visit(queryBuilder.Where);

            var orderBySql = new StringBuilder();
            if (queryBuilder.OrderBy != null)
                new SqlCodeGenerator(orderBySql, parameters).Visit(queryBuilder.OrderBy);

            return new SqlQuery
            {
                Select = selectSql.ToString(),
                Where = whereSql.ToString(),
                OrderBy = orderBySql.ToString(),
                Skip = queryBuilder.Skip,
                Take = queryBuilder.Take,
                Parameters = parameters.ToDictionary(x => x.Value, x => x.Key),
                ExecutionMethod = queryBuilder.Execution
            };
        }
    }
}