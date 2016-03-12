using System.Collections.Generic;
using System.Linq.Expressions;
using HybridDb.Config;
using HybridDb.Linq.Ast;

namespace HybridDb.Linq.Builders
{
    internal class OrderByBuilder : LambdaBuilder
    {
        public OrderByBuilder(DocumentDesign design, Stack<SqlExpression> ast) : base(design, ast) { }

        public static SqlColumnExpression Translate(DocumentDesign design, Expression expression)
        {
            var ast = new Stack<SqlExpression>();
            new OrderByBuilder(design, ast).Visit(expression);
            return (SqlColumnExpression) ast.Pop();
        }
    }
}