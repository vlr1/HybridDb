using System;
using System.Linq;
using System.Linq.Expressions;
using HybridDb.Linq.Parsers;

namespace HybridDb.Linq
{
    public static class QueryableEx
    {
        public static IQueryable<T> AsProjection<T>(this IQueryable query)
        {
            return query.Provider.CreateQuery<T>(query.OfType<T>().Expression);
        }

        public static IQueryable<T> Statistics<T>(this IQueryable<T> query, out QueryStats stats) where T : class
        {
            ((QueryProvider<T>)query.Provider).WriteStatisticsTo(out stats);
            return query;
        }

        public static bool In<T>(this T property, params T[] list)
        {
            return list.Contains(property);
        }

        public static IQueryable<T> SkipToId<T>(this IQueryable<T> query, Guid id, int pageSize) =>
            query.Provider.CreateQuery<T>(
                Expression.Call(typeof(QueryableEx)
                        .GetMethod(nameof(SkipToId))
                        .MakeGenericMethod(typeof(T)),
                    query.Expression,
                    Expression.Constant(id),
                    Expression.Constant(pageSize)));

        public static T Column<T>(this object parameter, string name)
        {
            throw new NotSupportedException("Only for building LINQ expressions");
        }
        
        public static T Index<T>(this object parameter)
        {
            throw new NotSupportedException("Only for building LINQ expressions");
        }
        
        internal static Translation Translate(this IQueryable query)
        {
            return Translate(query.Expression);
        }

        internal static Translation Translate(this Expression expression)
        {
            return new QueryTranslator().Translate(expression);
        }
    }
}