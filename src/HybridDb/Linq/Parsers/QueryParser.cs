using System;
using System.Linq.Expressions;
using HybridDb.Linq.Ast;

namespace HybridDb.Linq.Parsers
{
    internal class QueryParser : ExpressionVisitor
    {
        public Window Window { get; private set; }
        public bool Top1 { get; private set; }
        public SqlExpression Select { get; private set; }
        public SqlExpression Where { get; private set; }
        public SqlOrderByExpression OrderBy { get; private set; }
        public Translation.ExecutionSemantics Execution { get; set; }

        protected override Expression VisitMethodCall(MethodCallExpression expression)
        {
            Visit(expression.Arguments[0]);

            switch (expression.Method.Name)
            {
                case "Select":
                    Select = SelectParser.Translate(expression.Arguments[1]);
                    break;
                case "SingleOrDefault":
                    Execution = Translation.ExecutionSemantics.SingleOrDefault;
                    goto Take1;
                case "Single":
                    Execution = Translation.ExecutionSemantics.Single;
                    goto Take1;
                case "FirstOrDefault":
                    Execution = Translation.ExecutionSemantics.FirstOrDefault;
                    goto Take1;
                case "First":
                    Execution = Translation.ExecutionSemantics.First;
                    goto Take1;
                case "Take1":
                    Take1:
                    Top1 = true;
                    if (expression.Arguments.Count <= 1) break;
                    goto Where;
                case "Where":
                    Where:
                    var whereExpression = WhereParser.Translate(expression.Arguments[1]);
                    if (whereExpression == null)
                        break;

                    Where = Where != null
                                ? new SqlBinaryExpression(SqlNodeType.And, Where, whereExpression)
                                : whereExpression;
                    break;
                case "Skip":
                    Window = new SkipTake((int)((ConstantExpression)expression.Arguments[1]).Value, (Window as SkipTake)?.Take ?? 0);
                    break;
                case "Take":
                    Window = new SkipTake((Window as SkipTake)?.Skip ?? 0, (int)((ConstantExpression)expression.Arguments[1]).Value);
                    break;
                case "SkipToId":
                    Window = new SkipToId((Guid)((ConstantExpression)expression.Arguments[1]).Value, (int)((ConstantExpression)expression.Arguments[2]).Value);
                    break;
                case "OfType":
                    // Change of type is done else where
                    break;
                case "OrderBy":
                case "ThenBy":
                case "OrderByDescending":
                case "ThenByDescending":
                    var direction = expression.Method.Name.Contains("Descending")
                                        ? SqlOrderingExpression.Directions.Descending
                                        : SqlOrderingExpression.Directions.Ascending;

                    var orderByColumnExpression = OrderByVisitor.Translate(expression.Arguments[1]);
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