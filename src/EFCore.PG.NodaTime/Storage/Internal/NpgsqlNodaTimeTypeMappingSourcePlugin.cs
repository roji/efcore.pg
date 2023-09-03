using System.Collections.Concurrent;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;
using Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal.Mapping;

// ReSharper disable once CheckNamespace
namespace Npgsql.EntityFrameworkCore.PostgreSQL.Storage.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class NpgsqlNodaTimeTypeMappingSourcePlugin : IRelationalTypeMappingSourcePlugin
{
#if DEBUG
    internal static bool LegacyTimestampBehavior;
    internal static bool DisableDateTimeInfinityConversions;
#else
    internal static readonly bool LegacyTimestampBehavior;
    internal static readonly bool DisableDateTimeInfinityConversions;
#endif

    private readonly bool _supportsMultiranges;

    static NpgsqlNodaTimeTypeMappingSourcePlugin()
    {
        LegacyTimestampBehavior = AppContext.TryGetSwitch("Npgsql.EnableLegacyTimestampBehavior", out var enabled) && enabled;
        DisableDateTimeInfinityConversions = AppContext.TryGetSwitch("Npgsql.DisableDateTimeInfinityConversions", out enabled) && enabled;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public virtual ConcurrentDictionary<string, RelationalTypeMapping[]> StoreTypeMappings { get; }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public virtual ConcurrentDictionary<Type, RelationalTypeMapping> ClrTypeMappings { get; }

    #region TypeMapping

    private readonly TimestampLocalDateTimeMapping _timestampLocalDateTime = new();
    private readonly LegacyTimestampInstantMapping _legacyTimestampInstant = new();

    private readonly TimestampTzInstantMapping _timestamptzInstant = new();
    private readonly TimestampTzZonedDateTimeMapping _timestamptzZonedDateTime = new();
    private readonly TimestampTzOffsetDateTimeMapping _timestamptzOffsetDateTime = new();

    private readonly DateMapping _date = new();
    private readonly TimeMapping _time = new();
    private readonly TimeTzMapping _timetz = new();
    private readonly PeriodIntervalMapping _periodInterval = new();
    private readonly DurationIntervalMapping _durationInterval = new();

    // PostgreSQL has no native type for representing time zones - it just uses the IANA ID as text.
    private readonly DateTimeZoneMapping _timeZone = new("text");

    // Built-in ranges
    private readonly NpgsqlRangeTypeMapping _timestampLocalDateTimeRange;
    private readonly NpgsqlRangeTypeMapping _legacyTimestampInstantRange;
    private readonly NpgsqlRangeTypeMapping _timestamptzInstantRange;
    private readonly NpgsqlRangeTypeMapping _timestamptzZonedDateTimeRange;
    private readonly NpgsqlRangeTypeMapping _timestamptzOffsetDateTimeRange;
    private readonly NpgsqlRangeTypeMapping _dateRange;

    // NodaTime has DateInterval and Interval, which correspond to PostgreSQL daterange and tstzrange.
    // Users can still map the PG types to NpgsqlRange<LocalDate> and NpgsqlRange<Instant>, but the DateInterval/Interval mappings are
    // preferred.
    private readonly DateIntervalRangeMapping _dateIntervalRange = new();
    private readonly IntervalRangeMapping _intervalRange = new();

    #endregion

    /// <summary>
    /// Constructs an instance of the <see cref="NpgsqlNodaTimeTypeMappingSourcePlugin"/> class.
    /// </summary>
    public NpgsqlNodaTimeTypeMappingSourcePlugin(
        ISqlGenerationHelper sqlGenerationHelper,
        INpgsqlSingletonOptions options)
    {
        _supportsMultiranges = !options.IsPostgresVersionSet
            || options.IsPostgresVersionSet && options.PostgresVersion >= new Version(14, 0);

        _timestampLocalDateTimeRange
            = new NpgsqlRangeTypeMapping("tsrange", typeof(NpgsqlRange<LocalDateTime>), _timestampLocalDateTime, sqlGenerationHelper);
        _legacyTimestampInstantRange
            = new NpgsqlRangeTypeMapping("tsrange", typeof(NpgsqlRange<Instant>), _legacyTimestampInstant, sqlGenerationHelper);
        _timestamptzInstantRange
            = new NpgsqlRangeTypeMapping("tstzrange", typeof(NpgsqlRange<Instant>), _timestamptzInstant, sqlGenerationHelper);
        _timestamptzZonedDateTimeRange
            = new NpgsqlRangeTypeMapping("tstzrange", typeof(NpgsqlRange<ZonedDateTime>), _timestamptzZonedDateTime, sqlGenerationHelper);
        _timestamptzOffsetDateTimeRange
            = new NpgsqlRangeTypeMapping("tstzrange", typeof(NpgsqlRange<OffsetDateTime>), _timestamptzOffsetDateTime, sqlGenerationHelper);
        _dateRange
            = new NpgsqlRangeTypeMapping("daterange", typeof(NpgsqlRange<LocalDate>), _date, sqlGenerationHelper);

        var storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(StringComparer.OrdinalIgnoreCase)
        {
            {
                // We currently allow _legacyTimestampInstant even in non-legacy mode, since when upgrading to 6.0 with existing
                // migrations, model snapshots still contain old mappings (Instant mapped to timestamp), and EF Core's model differ
                // expects type mappings to be found for these. See https://github.com/dotnet/efcore/issues/26168.
                "timestamp without time zone", LegacyTimestampBehavior
                    ? new RelationalTypeMapping[] { _legacyTimestampInstant, _timestampLocalDateTime }
                    : new RelationalTypeMapping[] { _timestampLocalDateTime, _legacyTimestampInstant }
            },
            { "timestamp with time zone", new RelationalTypeMapping[] { _timestamptzInstant, _timestamptzZonedDateTime, _timestamptzOffsetDateTime } },
            { "date", new RelationalTypeMapping[] { _date } },
            { "time without time zone", new RelationalTypeMapping[] { _time } },
            { "time with time zone", new RelationalTypeMapping[] { _timetz } },
            { "interval", new RelationalTypeMapping[] { _periodInterval, _durationInterval } },

            { "tsrange", LegacyTimestampBehavior
                ? new RelationalTypeMapping[] { _legacyTimestampInstantRange, _timestampLocalDateTimeRange }
                : new RelationalTypeMapping[] { _timestampLocalDateTimeRange, _legacyTimestampInstantRange }
            },
            { "tstzrange", new RelationalTypeMapping[] { _intervalRange, _timestamptzInstantRange, _timestamptzZonedDateTimeRange, _timestamptzOffsetDateTimeRange } },
            { "daterange", new RelationalTypeMapping[] { _dateIntervalRange, _dateRange } }
        };

        // Set up aliases
        storeTypeMappings["timestamp"] = storeTypeMappings["timestamp without time zone"];
        storeTypeMappings["timestamptz"] = storeTypeMappings["timestamp with time zone"];
        storeTypeMappings["time"] = storeTypeMappings["time without time zone"];
        storeTypeMappings["timetz"] = storeTypeMappings["time with time zone"];

        var clrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            { typeof(Instant), LegacyTimestampBehavior ? _legacyTimestampInstant : _timestamptzInstant },

            { typeof(LocalDateTime), _timestampLocalDateTime },
            { typeof(ZonedDateTime), _timestamptzZonedDateTime },
            { typeof(OffsetDateTime), _timestamptzOffsetDateTime },
            { typeof(LocalDate), _date },
            { typeof(LocalTime), _time },
            { typeof(OffsetTime), _timetz },
            { typeof(Period), _periodInterval },
            { typeof(Duration), _durationInterval },
            // See DateTimeZone below

            { typeof(NpgsqlRange<Instant>), LegacyTimestampBehavior ? _legacyTimestampInstantRange : _timestamptzInstantRange },
            { typeof(NpgsqlRange<LocalDateTime>), _timestampLocalDateTimeRange },
            { typeof(NpgsqlRange<ZonedDateTime>), _timestamptzZonedDateTimeRange },
            { typeof(NpgsqlRange<OffsetDateTime>), _timestamptzOffsetDateTimeRange },
            { typeof(NpgsqlRange<LocalDate>), _dateRange },
            { typeof(DateInterval), _dateIntervalRange },
            { typeof(Interval), _intervalRange },
        };

        StoreTypeMappings = new ConcurrentDictionary<string, RelationalTypeMapping[]>(storeTypeMappings, StringComparer.OrdinalIgnoreCase);
        ClrTypeMappings = new ConcurrentDictionary<Type, RelationalTypeMapping>(clrTypeMappings);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public virtual RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        => FindBaseMapping(mappingInfo)?.Clone(mappingInfo)
            ?? FindMultirangeMapping(mappingInfo)?.Clone(mappingInfo);

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected virtual RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;
        var storeTypeNameBase = mappingInfo.StoreTypeNameBase;

        if (storeTypeName is not null)
        {
            if (StoreTypeMappings.TryGetValue(storeTypeName, out var mappings))
            {
                if (clrType is null)
                {
                    return mappings[0];
                }

                foreach (var m in mappings)
                {
                    if (m.ClrType == clrType)
                    {
                        return m;
                    }
                }

                return null;
            }

            if (StoreTypeMappings.TryGetValue(storeTypeNameBase!, out mappings))
            {
                if (clrType is null)
                {
                    return mappings[0].Clone(in mappingInfo);
                }

                foreach (var m in mappings)
                {
                    if (m.ClrType == clrType)
                    {
                        return m.Clone(in mappingInfo);
                    }
                }

                return null;
            }
        }

        if (clrType is not null)
        {
            if (ClrTypeMappings.TryGetValue(clrType, out var mapping))
            {
                return mapping;
            }

            if (clrType.IsAssignableTo(typeof(DateTimeZone)))
            {
                return _timeZone;
            }
        }

        return null;
    }

    /// <summary>
    ///     This resolves mappings for the specialized NodaTime <see cref="DateInterval" /> and <see cref="Interval" />, mapping them to
    ///     PostgreSQL datemultirange and tstzmultirange.
    /// </summary>
    /// <remarks>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </remarks>
    protected virtual RelationalTypeMapping? FindMultirangeMapping(RelationalTypeMappingInfo info)
    {
        if (!_supportsMultiranges)
        {
            return null;
        }

        var multirangeClrType = info.ClrType;
        RelationalTypeMapping? rangeMapping;
        string? rangeStoreType = null;

        var multirangeStoreType = info.StoreTypeName;
        if (multirangeStoreType is not null)
        {
            // If the multirange store type was explicitly specified, simply get the corresponding range type and build a type mapping
            // based on that; this notably means that the collection store type overrides the element's, if both were specified.
            // Note that if an ElementTypeMapping was provided, we still use that, cloning it to apply the range store type derived from
            // the multirange.
            rangeStoreType = multirangeStoreType switch
            {
                "tstzmultirange" => "tstzrange",
                "datemultirange" => "daterange",
                _ => null
            };

            if (rangeStoreType is null)
            {
                return null;
            }

            // TODO: Need to clone the element's type mapping, to preserve any configured converter/comparer/whatever; but we need to apply
            // both the inferred store type (overriding the element's), and also NpgsqlDbType
            // rangeMapping = (info.ElementTypeMapping is null
            //     ? FindMapping(rangeStoreType)
            //     : info.ElementTypeMapping.Clone(rangeStoreType, size: null));

            rangeMapping = FindMapping(new RelationalTypeMappingInfo(storeTypeName: rangeStoreType));
        }
        else
        {
            // A store type wasn't specified on the collection (see above). Either get the ElementTypeMapping configured on the property,
            // or infer it from the multirange's CLR type
            if (info.ElementTypeMapping is not null)
            {
                rangeMapping = info.ElementTypeMapping;
            }
            else
            {
                Type? rangeClrType = null;
                if (multirangeClrType is not null)
                {
                    rangeClrType = multirangeClrType.TryGetElementType(typeof(IEnumerable<>));

                    // E.g. Newtonsoft.Json's JToken is enumerable over itself, exclude that scenario to avoid stack overflow.
                    if (rangeClrType != typeof(Interval) && rangeClrType != typeof(DateInterval)
                        || multirangeClrType.GetGenericTypeImplementations(typeof(IDictionary<,>)).Any())
                    {
                        return null;
                    }
                }

                rangeMapping = (rangeClrType, rangeStoreType) switch
                {
                    (not null, not null) => FindMapping(new RelationalTypeMappingInfo { ClrType = rangeClrType, StoreTypeName = rangeStoreType }),
                    (not null, null) => FindMapping(new RelationalTypeMappingInfo { ClrType = rangeClrType }),
                    (null, not null) => FindMapping(new RelationalTypeMappingInfo { StoreTypeName = rangeStoreType }),
                    _ => null
                };
            }
        }

        if (rangeMapping is not (DateIntervalRangeMapping or IntervalRangeMapping))
        {
            return null;
        }

        // TODO: Consider returning List<T> by default for scaffolding, more useful, #2758
        multirangeClrType ??= rangeMapping.ClrType.MakeArrayType();

        return rangeMapping switch
        {
            DateIntervalRangeMapping m => new DateIntervalMultirangeMapping(multirangeClrType, m),
            IntervalRangeMapping m => new IntervalMultirangeMapping(multirangeClrType, m),
            _ => throw new UnreachableException()
        };
    }
}
