using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query.ExpressionTranslators.Internal
{
    public class NpgsqlDateTimeMethodTranslator : IMethodCallTranslator
    {
        private static readonly Dictionary<MethodInfo, string> MethodInfoDatePartMapping = new()
        {
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddYears), new[] { typeof(int) })!, "years" },
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMonths), new[] { typeof(int) })!, "months" },
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), new[] { typeof(double) })!, "days" },
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), new[] { typeof(double) })!, "hours" },
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), new[] { typeof(double) })!, "mins" },
            { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), new[] { typeof(double) })!, "secs" },
            //{ typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMilliseconds), new[] { typeof(double) })!, "milliseconds" },

            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddYears), new[] { typeof(int) })!, "years" },
            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMonths), new[] { typeof(int) })!, "months" },
            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddDays), new[] { typeof(double) })!, "days" },
            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddHours), new[] { typeof(double) })!, "hours" },
            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMinutes), new[] { typeof(double) })!, "mins" },
            { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddSeconds), new[] { typeof(double) })!, "secs" },
            //{ typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMilliseconds), new[] { typeof(double) })!, "milliseconds" }

            { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), new[] { typeof(int) })!, "years" },
            { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), new[] { typeof(int) })!, "months" },
            { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), new[] { typeof(int) })!, "days" },

            { typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.AddHours), new[] { typeof(int) })!, "hours" },
            { typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.AddMinutes), new[] { typeof(int) })!, "mins" },
        };

        private static readonly MethodInfo DateTimeToUniversalTimeMethod
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.ToUniversalTime), Array.Empty<Type>())!;
        private static readonly MethodInfo DateTimeToLocalTimeMethod
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.ToLocalTime), Array.Empty<Type>())!;

        private static readonly MethodInfo TimeOnlyIsBetweenMethod
            = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.IsBetween), new[] { typeof(TimeOnly), typeof(TimeOnly) })!;
        private static readonly MethodInfo TimeOnlyAddTimeSpanMethod
            = typeof(TimeOnly).GetRuntimeMethod(nameof(TimeOnly.Add), new[] { typeof(TimeSpan) })!;

        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly RelationalTypeMapping _timestampMapping;
        private readonly RelationalTypeMapping _timestampTzMapping;
        private readonly RelationalTypeMapping _intervalMapping;
        private readonly RelationalTypeMapping _textMapping;

        public NpgsqlDateTimeMethodTranslator(
            IRelationalTypeMappingSource typeMappingSource,
            ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _timestampMapping = typeMappingSource.FindMapping("timestamp without time zone")!;
            _timestampTzMapping = typeMappingSource.FindMapping("timestamp with time zone")!;
            _intervalMapping = typeMappingSource.FindMapping("interval")!;
            _textMapping = typeMappingSource.FindMapping("text")!;
        }

        /// <inheritdoc />
        public virtual SqlExpression? Translate(
            SqlExpression? instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (instance is null)
            {
                return null;
            }

            if (TranslateDatePart(instance, method, arguments) is { } translated)
            {
                return translated;
            }

            if (method.DeclaringType == typeof(DateTime))
            {
                if (method == DateTimeToUniversalTimeMethod)
                {
                    return _sqlExpressionFactory.Convert(instance, method.ReturnType, _timestampTzMapping);
                }
                if (method == DateTimeToLocalTimeMethod)
                {
                    return _sqlExpressionFactory.Convert(instance, method.ReturnType, _timestampMapping);
                }
            }
            else if (method.DeclaringType == typeof(TimeOnly))
            {
                if (method == TimeOnlyIsBetweenMethod)
                {
                    return _sqlExpressionFactory.And(
                        _sqlExpressionFactory.GreaterThanOrEqual(instance, arguments[0]),
                        _sqlExpressionFactory.LessThan(instance, arguments[1]));
                }

                if (method == TimeOnlyAddTimeSpanMethod)
                {
                    return _sqlExpressionFactory.Add(instance, arguments[0]);
                }
            }

            return null;
        }

        private SqlExpression? TranslateDatePart(
            SqlExpression instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments)
        {
            if (!MethodInfoDatePartMapping.TryGetValue(method, out var datePart))
                return null;

            if (arguments[0] is not { } interval)
                return null;

            // Note: ideally we'd simply generate a PostgreSQL interval expression, but the .NET mapping of that is TimeSpan,
            // which does not work for months, years, etc. So we generate special fragments instead.
            if (interval is SqlConstantExpression constantExpression)
            {
                // We generate constant intervals as INTERVAL '1 days'
                if (constantExpression.Type == typeof(double) &&
                    ((double)constantExpression.Value! >= int.MaxValue ||
                     (double)constantExpression.Value <= int.MinValue))
                {
                    return null;
                }

                interval = _sqlExpressionFactory.Fragment(FormattableString.Invariant($"INTERVAL '{constantExpression.Value} {datePart}'"));
            }
            else
            {
                // For non-constants, we can't parameterize INTERVAL '1 days'. Instead, we use CAST($1 || ' days' AS interval).
                // Note that a make_interval() function also exists, but accepts only int (for all fields except for
                // seconds), so we don't use it.
                // Note: we instantiate SqlBinaryExpression manually rather than via sqlExpressionFactory because
                // of the non-standard Add expression (concatenate int with text)
                interval = _sqlExpressionFactory.Convert(
                    new SqlBinaryExpression(
                        ExpressionType.Add,
                        _sqlExpressionFactory.Convert(interval, typeof(string), _textMapping),
                        _sqlExpressionFactory.Constant(' ' + datePart, _textMapping),
                        typeof(string),
                        _textMapping),
                    typeof(TimeSpan),
                    _intervalMapping);
            }

            return _sqlExpressionFactory.Add(instance, interval, instance.TypeMapping);
        }
    }
}
