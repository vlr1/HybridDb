﻿using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace HybridDb.Linq.Parsers
{

    //TODO:
    //public class QueryTranslator
    //{
    //    public SqlSelectStatement Translate(Expression expression)
    //    {
    //        var queryVisitor = new QueryParser();
            
    //        // parameters are indexed by value to ease reusing params by value
    //        var parameters = new Dictionary<object, string>();

    //        queryVisitor.Visit(expression);

    //        var selectSql = new StringBuilder();
    //        if (queryVisitor.Select != null)
    //            new SqlCodeGenerator(selectSql, parameters).Visit(queryVisitor.Select);

    //        var whereSql = new StringBuilder();
    //        if (queryVisitor.Where != null)
    //            new SqlCodeGenerator(whereSql, parameters).Visit(queryVisitor.Where);

    //        var orderBySql = new StringBuilder();
    //        if (queryVisitor.OrderBy != null)
    //            new SqlCodeGenerator(orderBySql, parameters).Visit(queryVisitor.OrderBy);

    //        return new SqlSelectStatement
    //        {
    //            Select = selectSql.ToString(),
    //            Where = whereSql.ToString(),
    //            ProjectAs = queryVisitor.ProjectAs,
    //            OrderBy = orderBySql.ToString(),
    //            Skip = queryVisitor.Skip,
    //            Take = queryVisitor.Take,
    //            Parameters = parameters.ToDictionary(x => x.Value, x => x.Key),
    //            ExecutionMethod = queryVisitor.Execution
    //        };
    //    }
    //}
}