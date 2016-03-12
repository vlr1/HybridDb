using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Xml;
using HybridDb.Config;
using HybridDb.Linq.Ast;

namespace HybridDb.Linq.Builders
{
    internal class WhereBuilder : LambdaBuilder
    {
        public WhereBuilder(DocumentDesign design, Stack<SqlExpression> ast) : base(design, ast)
        {
        }

        public SqlExpression Result
        {
            get { return ast.Peek(); }
        }

        public static SqlExpression Translate(DocumentDesign design, Expression expression)
        {
            var ast = new Stack<SqlExpression>();

            new WhereBuilder(design, ast).Visit(expression);

            if (ast.Count == 0)
                return null;

            var sqlExpression = ast.Pop();
            sqlExpression = new ImplicitBooleanPredicatePropagator().Visit(sqlExpression);
            sqlExpression = new NullCheckPropagator().Visit(sqlExpression);

            return sqlExpression;
        }

        protected override Expression VisitBinary(BinaryExpression expression)
        {
            Visit(expression.Left);
            Visit(expression.Right);
            
            var right = ast.Pop();
            var left = ast.Pop();

            SqlNodeType nodeType;
            switch (expression.NodeType)
            {
                case ExpressionType.And:
                    nodeType = SqlNodeType.BitwiseAnd;
                    break;
                case ExpressionType.AndAlso:
                    nodeType = SqlNodeType.And;
                    break;
                case ExpressionType.Or:
                    nodeType = SqlNodeType.BitwiseOr;
                    break;
                case ExpressionType.OrElse:
                    nodeType = SqlNodeType.Or;
                    break;
                case ExpressionType.LessThan:
                    nodeType = SqlNodeType.LessThan;
                    break;
                case ExpressionType.LessThanOrEqual:
                    nodeType = SqlNodeType.LessThanOrEqual;
                    break;
                case ExpressionType.GreaterThan:
                    nodeType = SqlNodeType.GreaterThan;
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    nodeType = SqlNodeType.GreaterThanOrEqual;
                    break;
                case ExpressionType.Equal:
                    nodeType = SqlNodeType.Equal;
                    break;
                case ExpressionType.NotEqual:
                    nodeType = SqlNodeType.NotEqual;
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", expression.NodeType));
            }

            ast.Push(new SqlBinaryExpression(nodeType, left, right));

            return expression;
        }

        protected override Expression VisitUnary(UnaryExpression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Not:
                    Visit(expression.Operand);
                    ast.Push(new SqlNotExpression(ast.Pop()));
                    break;
                case ExpressionType.Quote:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    Visit(expression.Operand);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", expression.NodeType));
            }

            return expression;
        }

        protected override void VisitColumnMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "StartsWith":
                    ast.Push(new SqlBinaryExpression(SqlNodeType.LikeStartsWith, ast.Pop(), ast.Pop()));
                    break;
                case "Contains":
                    ast.Push(new SqlBinaryExpression(SqlNodeType.LikeContains, ast.Pop(), ast.Pop()));
                    break;
                case "In":
                    var column = ast.Pop();
                    var set = (SqlConstantExpression)ast.Pop();
                    if (((IEnumerable) set.Value).Cast<object>().Any())
                    {
                        ast.Push(new SqlBinaryExpression(SqlNodeType.In, column, set));
                    }
                    else
                    {
                        ast.Push(new SqlConstantExpression(typeof(bool), false));
                    }
                    break;
                case "Any":
                    if (!IsXmlColumn(((SqlColumnExpression) ast.Peek()))) 
                        goto default;
                    
                    ast.Push(new SqlBinaryExpression(
                        SqlNodeType.Equal,
                        new SqlXQueryExistExpression(
                            (SqlColumnExpression) ast.Pop(),
                            new PredicateBuilder().BuildAnyPredicate2(expression)),
                        new SqlConstantExpression(typeof (int), 1)));
                    break;
                default:
                    base.VisitColumnMethodCall(expression);
                    break;
            }
        }

        bool IsXmlColumn(SqlColumnExpression expression)
        {
            var column = design.Table[((SqlColumnExpression) ast.Peek()).ColumnName];
            return column != null && SqlTypeMap.Convert(column).DbType == DbType.Xml;
        }
    }

    public class PredicateBuilder
    {
        private readonly Stack<string> paths = new Stack<string>();
        private string CurrentPath
        {
            get { return paths.Peek(); }
        }
        private string GetFreeVariable()
        {
            int index = paths.Count;
            return "$" + ((char)(64 + index));
        }

        public string TranslateToWhere(Expression predicate)
        {
            paths.Push("");

            var x = predicate as UnaryExpression;
            var lambdaExpression = x.Operand as LambdaExpression;
            var result = BuildPredicate(lambdaExpression.Body);

            paths.Pop();

            return result;
        }

        public string BuildPredicate(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Call:
                    return BuildPredicateCall(expression);
                case ExpressionType.Convert:
                    return BuildPredicateConvert(expression);
                case ExpressionType.Constant:
                    return BuildPredicateConstant(expression);
                case ExpressionType.MemberAccess:
                    return BuildPredicateMemberAccess(expression);
                case ExpressionType.TypeIs:
                    return BuildPredicateTypeIs(expression);
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.Add:
                case ExpressionType.Subtract:
                case ExpressionType.Multiply:
                case ExpressionType.Divide:

                    return BuildPredicateBinaryExpression(expression);
                default:
                    throw new NotSupportedException("Unknown expression type");
            }
        }

        private string BuildPredicateCall(Expression expression)
        {
            var methodCallExpression = expression as MethodCallExpression;

            if (methodCallExpression.Method.DeclaringType == typeof(Enumerable) ||
                methodCallExpression.Method.DeclaringType == typeof(Queryable))
            {
                switch (methodCallExpression.Method.Name)
                {
                    case "Any":
                        return BuildAnyPredicate(methodCallExpression);
                    case "Count":
                        return BuildCountPredicate(methodCallExpression);
                    case "Sum":
                    case "Min":
                    case "Max":
                    case "Average":
                        return BuildAggregatePredicate(methodCallExpression,
                                                       XQueryMapping.Functions[methodCallExpression.Method.Name]);
                    default:
                        break;
                }
            }

            throw new NotSupportedException("Unknown method");
        }

        private string BuildCountPredicate(MethodCallExpression methodCallExpression)
        {
            string propertyPath = GetPropertyPath(methodCallExpression.Arguments[0]);
            string predicate = string.Format("fn:count({0})", propertyPath);
            return predicate;
        }

        public string BuildAnyPredicate(MethodCallExpression methodCallExpression)
        {
            var rootPath = BuildPredicate(methodCallExpression.Arguments[0]);
            var lambda = methodCallExpression.Arguments[1] as LambdaExpression;
            var body = lambda.Body;
            var part = BuildPredicate(body);
            var propertyPath = GetPropertyPath(methodCallExpression.Arguments[0]);
            var predicate = string.Format("item/{0}[{1}]", propertyPath, part);
            return predicate;
        }

        public string BuildAnyPredicate2(MethodCallExpression methodCallExpression)
        {
            paths.Push("");
            var lambda = methodCallExpression.Arguments[1] as LambdaExpression;
            var body = lambda.Body;
            var part = BuildPredicate(body);
            var predicate = string.Format("root/item[{0}]", part);
            return predicate;
        }

        private string BuildAggregatePredicate(MethodCallExpression methodCallExpression, string functionName)
        {
            string propertyPath = BuildPredicate(methodCallExpression.Arguments[0]);
            var lambda = methodCallExpression.Arguments[1] as LambdaExpression;
            Expression body = lambda.Body;
            string variable = GetFreeVariable();
            paths.Push(variable + "/");
            string part = BuildPredicate(body);
            paths.Pop();
            string predicate = string.Format("{0}( for {1} in {2}/element return {3})", functionName, variable,
                                             propertyPath,
                                             part);
            return predicate;
        }



        private string BuildPredicateConvert(Expression expression)
        {
            var convertExpression = expression as UnaryExpression;
            return BuildPredicate(convertExpression.Operand);
        }

        private string BuildPredicateBinaryExpression(Expression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            var op = XQueryMapping.Operators[expression.NodeType];


            var rightIsNull = IsConstantNull(binaryExpression.Right);

            if (rightIsNull)
            {
                string left = GetPropertyPath(binaryExpression.Left);
                return string.Format("{0}/@type[.{1}\"null\"]", left, op.Code);
            }
            else if (op.IsBool && CanReduceToDot(binaryExpression))
            {
                string dot = ReduceToDot(binaryExpression);
                string path = ReduceToDotPath(binaryExpression);
                reducePropertyPath = null;
                return string.Format("{0}[{1}]", path, dot);
            }
            else
            {
                reducePropertyPath = null;
                string left = BuildPredicate(binaryExpression.Left);
                string right = BuildPredicate(binaryExpression.Right);
                return string.Format("({0} {1} {2})", left, op.Code, right);
            }
        }

        private string ReduceToDotPath(Expression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            if (binaryExpression != null)
            {
                var op = XQueryMapping.Operators[expression.NodeType];
                var left = ReduceToDotPath(binaryExpression.Left);
                if (left != null)
                    return left;
                var right = ReduceToDotPath(binaryExpression.Right);
                return right;
            }

            if (expression is UnaryExpression)
            {
                return GetPropertyPath(((UnaryExpression)expression).Operand);
            }

            if (expression is ConstantExpression)
            {
                return null;
            }

            if (expression is MemberExpression)
            {
                return GetPropertyPath(expression);
            }

            throw new NotSupportedException();
        }

        private string ReduceToDot(Expression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            if (binaryExpression != null)
            {
                var op = XQueryMapping.Operators[expression.NodeType];
                var left = ReduceToDot(binaryExpression.Left);
                var right = ReduceToDot(binaryExpression.Right);
                return string.Format("{0}{1}{2}", left, op.Code, right);
            }

            if (expression is UnaryExpression)
            {
                return ReduceToDot(((UnaryExpression)expression).Operand);
            }

            if (expression is ConstantExpression)
            {
                return BuildPredicateConstant(expression);
            }

            if (expression is MemberExpression)
            {
                var memberExpression = expression as MemberExpression;
                if (memberExpression.Member.DeclaringType == typeof(DateTime))
                    return XQueryMapping.BuildLiteral(DateTime.Now);

                if (memberExpression.Expression.Type.Name.StartsWith("<>"))
                {
                    var c = memberExpression.Expression as ConstantExpression;
                    var h = c.Value;
                    var result = h.GetType().GetFields().First().GetValue(h);
                    return XQueryMapping.BuildLiteral(result);
                }

                return ".";
            }

            throw new NotSupportedException();
        }

        private string reducePropertyPath = null;
        private bool CanReduceToDot(Expression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            if (binaryExpression != null)
            {
                if (CanReduceToDot(binaryExpression.Left) && CanReduceToDot(binaryExpression.Right))
                    return true;

                return false;
            }

            if (expression is UnaryExpression)
            {
                return CanReduceToDot(((UnaryExpression)expression).Operand);
            }

            if (expression is ConstantExpression)
                return true;

            if (expression is MemberExpression)
            {
                var memberExpression = expression as MemberExpression;

                if (memberExpression.Member.DeclaringType == typeof(DateTime))
                    return true;

                if (memberExpression.Expression.Type.Name.StartsWith("<>"))
                    return true;

                string currentPropertyPath = GetPropertyPath(expression);
                if (reducePropertyPath == null)
                    reducePropertyPath = currentPropertyPath;

                if (currentPropertyPath == reducePropertyPath)
                    return true;
                else
                    return false;
            }

            return false;
        }


        private string GetPropertyPath(Expression expression)
        {
            return BuildPredicateMemberAccessReq(expression);
        }

        private static bool IsConstantNull(Expression expression)
        {
            var rightAsUnary = expression as UnaryExpression;
            ConstantExpression rightAsConstant = rightAsUnary != null
                                                     ? rightAsUnary.Operand as ConstantExpression
                                                     : null;
            var rightIsNull = rightAsConstant != null && rightAsConstant.Value == null;
            return rightIsNull;
        }

        private string BuildPredicateTypeIs(Expression expression)
        {
            var typeBinaryExpression = expression as TypeBinaryExpression;
            string left = GetPropertyPath(typeBinaryExpression.Expression);
            string typeName = typeBinaryExpression.TypeOperand.SerializedName();

            //check if type attrib equals typename OR if typename exists in metadata type array
            string query = string.Format("{0}[(@type[.=\"{1}\"] or __meta[type[. = \"{1}\"]])]", left, typeName);
            return query;
        }

        private string BuildPredicateMemberAccess(Expression expression)
        {
            var memberExpression = expression as MemberExpression;
            string memberName = memberExpression.Member.Name;

            if (memberExpression.Member.DeclaringType == typeof(DateTime))
            {
                if (memberName == "Now")
                    return XQueryMapping.BuildLiteral(DateTime.Now);
            }

            return string.Format("({0})[1]", BuildPredicateMemberAccessReq(expression));
        }

        private string BuildPredicateMemberAccessReq(Expression expression)
        {
            var memberExpression = expression as MemberExpression;
            string memberName = memberExpression.Member.Name;

            string current = string.Format("{0}", memberName);


            //if (memberExpression.Type == typeof(bool) || memberExpression.Type == typeof(bool?))
            //    current += "/x:bool";

            //if (memberExpression.Type == typeof(string) || memberExpression.Type == typeof(Guid))
            //    current += "/x:str";

            //if (memberExpression.Type == typeof(DateTime) || memberExpression.Type == typeof(DateTime?))
            //    current += "/x:dt";

            //if (memberExpression.Type == typeof(int) || memberExpression.Type == typeof(int?) || memberExpression.Type.IsEnum)
            //    current += "/x:int";

            //if (memberExpression.Type == typeof(double) || memberExpression.Type == typeof(double?) || memberExpression.Type == typeof(decimal) || memberExpression.Type == typeof(decimal?))
            //    current += "/x:dec";


            string prev = "";
            if (memberExpression.Expression is MemberExpression)
                prev = BuildPredicateMemberAccessReq(memberExpression.Expression) + "/";
            else
                prev = CurrentPath;

            return prev + current;
        }

        private string BuildPredicateConstant(Expression expression)
        {
            var constantExpression = expression as ConstantExpression;
            object value = constantExpression.Value;
            return XQueryMapping.BuildLiteral(value);
        }

        public string TranslateToOfType(MethodCallExpression node)
        {
            Type ofType = node.Method.GetGenericArguments()[0] as Type;

            string typeName = ofType.SerializedName();

            //check if type attrib equals typename OR if typename exists in metadata type array
            string query = string.Format("(@type[.=\"{0}\"] or __meta[type[. = \"{0}\"]])", typeName);
            return query;
        }
    }

    public class OperatorInfo
    {
        public string Code { get; set; }
        public bool IsBool { get; set; }
    }
    public static class XQueryMapping
    {
        public static readonly Dictionary<ExpressionType, OperatorInfo> Operators = new Dictionary<ExpressionType, OperatorInfo>
                {
                    {ExpressionType.AndAlso, new OperatorInfo{Code = "and" , IsBool = true }},
                    {ExpressionType.OrElse, new OperatorInfo{Code = "or" , IsBool = true }},
                    {ExpressionType.NotEqual, new OperatorInfo{Code = "!=" , IsBool = true }},
                    {ExpressionType.LessThan, new OperatorInfo{Code = "<" , IsBool = true }},
                    {ExpressionType.LessThanOrEqual, new OperatorInfo{Code = "<=" , IsBool = true }},
                    {ExpressionType.GreaterThan, new OperatorInfo{Code = ">" , IsBool = true }},
                    {ExpressionType.GreaterThanOrEqual, new OperatorInfo{Code = ">=" , IsBool = true }},
                    {ExpressionType.Equal, new OperatorInfo{Code = "=" , IsBool = true }},
                    {ExpressionType.Add, new OperatorInfo{Code = "+" , IsBool = false }},
                    {ExpressionType.Subtract, new OperatorInfo{Code = "-" , IsBool = false }},
                    {ExpressionType.Divide, new OperatorInfo{Code = "/" , IsBool = false }},
                    {ExpressionType.Multiply, new OperatorInfo{Code = "*" , IsBool = false }},
                };


        public static readonly Dictionary<string, string> Functions = new Dictionary<string, string>
                                                                           {
                                                                               {"Sum", "fn:sum"},
                                                                               {"Max", "fn:max"},
                                                                               {"Min", "fn:min"},
                                                                               {"Average", "fn:avg"},
                                                                           };

        public static readonly string xsTrue = "fn:true()";
        public static readonly string xsFalse = "fn:false()";

        public static string BuildLiteral(object value)
        {
            if (value is Guid)
                return "\"" + SerializeString(value.ToString()) + "\"";
            if (value is string)
                return "\"" + SerializeString((string)value) + "\"";
            if (value is int)
                return string.Format("{0}", SerializeDecimal((int)value));
            if (value is decimal)
                return string.Format("{0}", SerializeDecimal((decimal)value));
            if (value is DateTime)
                return string.Format("xs:dateTime(\"{0}\")", SerializeDateTime((DateTime)value));
            if (value is bool)
                if ((bool)value)
                    return XQueryMapping.xsTrue;
                else
                    return XQueryMapping.xsFalse;

            return value.ToString();
        }



        public static string SerializeDateTime(DateTime value)
        {
            return XmlConvert.ToString(value, XmlDateTimeSerializationMode.Local);
        }

        public static string SerializeInt(int value)
        {
            return XmlConvert.ToString(value);
        }

        public static string SerializeDouble(double value)
        {
            return XmlConvert.ToString(value);
        }

        public static string SerializeDecimal(decimal value)
        {
            return XmlConvert.ToString(value);
        }

        public static string SerializeBool(bool value)
        {
            return XmlConvert.ToString(value);
        }

        public static string SerializeGuid(Guid value)
        {
            return XmlConvert.ToString(value);
        }

        public static string SerializeString(string value)
        {
            return value;
        }
    }

    public static class TypeExtensions
    {
        public static string SerializedName(this Type self)
        {
            return string.Format("{0}, {1}", self.FullName,
                                 self.Assembly.FullName.Substring(0, self.Assembly.FullName.IndexOf(",")));
        }
    }

}