using Microsoft.EntityFrameworkCore.Query.Internal;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Extensions;

/// <summary>
///     Provides Npgsql-specific extension methods on <see cref="IQueryable" />.
/// </summary>
public static class NpgsqlQueryableExtensions
{
    internal static readonly MethodInfo IncludeMethodInfo
        = typeof(NpgsqlQueryableExtensions).GetTypeInfo().GetDeclaredMethods(nameof(WithNullFirst)).Single();

    /// <summary>
    ///     Defines the null sort ordering for the
    /// </summary>
    /// <param name="source"></param>
    /// <param name="nullsFirst"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static IOrderedQueryable<T> WithNullFirst<T>(this IOrderedQueryable<T> source, bool nullsFirst)
        => source.Provider is EntityQueryProvider
            ? (IOrderedQueryable<T>)source.Provider.CreateQuery<T>(
                Expression.Call(
                    instance: null,
                    method: IncludeMethodInfo.MakeGenericMethod(typeof(T)),
                    arg0: source.Expression,
                    arg1: Expression.Constant(nullsFirst)))
            : source;
}
