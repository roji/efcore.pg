using Microsoft.EntityFrameworkCore.Query.Internal;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

/// <summary>
///     Provides Npgsql-specific extension methods on <see cref="IQueryable" />.
/// </summary>
public static class NpgsqlQueryableExtensions
{
    internal static readonly MethodInfo QueryableWithNullsFirstMethodInfo
            // = null!;
        = typeof(NpgsqlQueryableExtensions).GetTypeInfo().GetDeclaredMethods(nameof(WithNullsFirst))
        .Single(mi => mi.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>));

    internal static readonly MethodInfo EnumerableWithNullsFirstMethodInfo
        // = null!;
        = typeof(NpgsqlQueryableExtensions).GetTypeInfo().GetDeclaredMethods(nameof(WithNullsFirst))
        .Single(mi => mi.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IOrderedEnumerable<>));

    /// <summary>
    ///     Defines the null sort ordering for the
    /// </summary>
    /// <param name="source"></param>
    /// <param name="nullsFirst"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static IOrderedQueryable<T> WithNullsFirst<T>(this IOrderedQueryable<T> source, bool nullsFirst)
        => source.Provider is EntityQueryProvider
            ? (IOrderedQueryable<T>)source.Provider.CreateQuery<T>(
                Expression.Call(
                    instance: null,
                    method: QueryableWithNullsFirstMethodInfo.MakeGenericMethod(typeof(T)),
                    arg0: source.Expression,
                    arg1: Expression.Constant(nullsFirst)))
            : source;

    /// <summary>
    ///     Defines the null sort ordering for the
    /// </summary>
    /// <param name="source"></param>
    /// <param name="nullsFirst"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static IOrderedEnumerable<T> WithNullsFirst<T>(this IOrderedEnumerable<T> source, bool nullsFirst)
        => throw new InvalidOperationException(CoreStrings.FunctionOnClient(nameof(WithNullsFirst)));
}
