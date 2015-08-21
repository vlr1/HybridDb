namespace HybridDb.Linq.Ast
{
    public class SqlXQueryExistExpression : SqlExpression
    {
        public SqlXQueryExistExpression(SqlColumnExpression column, string query)
        {
            Column = column;
            Query = query;
        }

        public SqlColumnExpression Column { get; private set; }
        public string Query { get; private set; }

        public override SqlNodeType NodeType
        {
            get { return SqlNodeType.XQueryExist; }
        }
    }
}