using System;
using System.Linq.Expressions;
using HybridDb.Config;
using HybridDb.Linq.Ast;

namespace HybridDb.Linq.Builders
{
    internal class QueryBuilder : ExpressionVisitor
    {
        readonly Configuration configuration;

        public QueryBuilder(Configuration configuration)
        {
            this.configuration = configuration;
        }

        public int Skip { get; private set; }
        public int Take { get; private set; }
        public SqlExpression Select { get; private set; }
        public SqlExpression Where { get; private set; }
        public SqlOrderByExpression OrderBy { get; private set; }
        public SqlQuery.ExecutionSemantics Execution { get; set; }

        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            Visit(expression.Arguments[0]);

            switch (expression.Method.Name)
            {
                case "Select":
                    Select = SelectBuilder.Translate(expression.Arguments[1]);
                    break;
                case "SingleOrDefault":
                    Execution = SqlQuery.ExecutionSemantics.SingleOrDefault;
                    goto Take1;
                case "Single":
                    Execution = SqlQuery.ExecutionSemantics.Single;
                    goto Take1;
                case "FirstOrDefault":
                    Execution = SqlQuery.ExecutionSemantics.FirstOrDefault;
                    goto Take1;
                case "First":
                    Execution = SqlQuery.ExecutionSemantics.First;
                    goto Take1;
                case "Take1":
                    Take1:
                    Take = 1;
                    if (expression.Arguments.Count > 1) goto Where;
                    break;
                case "Where":
                    Where:
                    var whereExpression = WhereBuilder.Translate(expression.Arguments[1]);
                    if (whereExpression == null)
                        break;

                    Where = Where != null
                                ? new SqlBinaryExpression(SqlNodeType.And, Where, whereExpression)
                                : whereExpression;
                    break;
                case "Skip":
                    Skip = (int) ((ConstantExpression) expression.Arguments[1]).Value;
                    break;
                case "Take":
                    Take = (int) ((ConstantExpression) expression.Arguments[1]).Value;
                    break;
                case "OfType":
                    // Change of type is done elsewhere
                    break;
                case "OrderBy":
                case "ThenBy":
                case "OrderByDescending":
                case "ThenByDescending":
                    var direction = expression.Method.Name.Contains("Descending")
                                        ? SqlOrderingExpression.Directions.Descending
                                        : SqlOrderingExpression.Directions.Ascending;

                    var orderByColumnExpression = OrderByBuilder.Translate(expression.Arguments[1]);
                    var orderingExpression = new SqlOrderingExpression(direction, orderByColumnExpression);
                    OrderBy = OrderBy != null
                                  ? new SqlOrderByExpression(OrderBy.Columns.Concat(orderingExpression))
                                  : new SqlOrderByExpression(orderingExpression.AsEnumerable());
                    break;
                default:
                    throw new NotSupportedException(string.Format("The method {0} is not supported", expression.Method.Name));
            }
            return expression;
        }
    }
}