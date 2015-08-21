using System.Collections.Generic;
using System.Linq.Expressions;
using HybridDb.Linq.Ast;

namespace HybridDb.Linq.Builders
{
    internal class OrderByBuilder : LambdaBuilder
    {
        public OrderByBuilder(Stack<SqlExpression> ast) : base(ast) {}

        public static SqlColumnExpression Translate(Expression expression)
        {
            var ast = new Stack<SqlExpression>();
            new OrderByBuilder(ast).Visit(expression);
            return (SqlColumnExpression) ast.Pop();
        }
    }
}