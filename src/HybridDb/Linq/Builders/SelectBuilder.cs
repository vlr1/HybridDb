using System.Collections.Generic;
using System.Linq.Expressions;
using HybridDb.Config;
using HybridDb.Linq.Ast;
using System.Linq;

namespace HybridDb.Linq.Builders
{
    internal class SelectBuilder : LambdaBuilder
    {
        public SelectBuilder(DocumentDesign design, Stack<SqlExpression> ast) : base(design, ast) { }

        public static SqlExpression Translate(DocumentDesign design, Expression expression)
        {
            var operations = new Stack<SqlExpression>();
            new SelectBuilder(design, operations).Visit(expression);
            return operations.Pop();
        }
        
        protected override Expression VisitNew(NewExpression expression)
        {
            var projections = new SqlProjectionExpression[expression.Arguments.Count];
            for (int i = 0; i < expression.Arguments.Count; i++)
            {
                Visit(expression.Arguments[i]);
                projections[i] = new SqlProjectionExpression((SqlColumnExpression) ast.Pop(), expression.Members[i].Name);
            }

            ast.Push(new SqlSelectExpression(projections));
            return expression;
        }

        protected override Expression VisitMemberInit(MemberInitExpression expression)
        {
            var projections = new List<SqlProjectionExpression>();
            foreach (var memberBinding in expression.Bindings)
            {
                var property = memberBinding as MemberAssignment;
                if (property == null)
                    continue;

                Visit(property.Expression);
                projections.Add(new SqlProjectionExpression((SqlColumnExpression)ast.Pop(), property.Member.Name));
            }

            ast.Push(new SqlSelectExpression(projections));

            return expression;
        }
    }
}