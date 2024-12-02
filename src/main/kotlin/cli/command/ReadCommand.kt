package dynq.cli.command

import dynq.cli.anno.CliCommand
import dynq.cli.anno.CliOption
import jakarta.validation.constraints.*

@CliCommand(root = true)
interface ReadCommand : Command {

    @CliOption(
        long = TABLE_NAME,
        short = 'f'
    )
    @Pattern(regexp = "[a-zA-Z_\\-.]*")
    @Size(min = 3, max = 255)
    fun tableName(): String

    @CliOption(
        long = TRANSFORM,
        short = 't'
    )
    fun transform(): String?

    @CliOption(
        long = WHERE,
        short = 'w'
    )
    fun where(): String?

    @CliOption(
        long = PRETRANSFORM,
        short = 'T'
    )
    fun pretransform(): String?

    @CliOption(
        long = PARTITION_KEY,
        short = 'P'
    )
    fun partitionKey(): String?

    @CliOption(
        long = SORT_KEY,
        short = 'S',
        requires = [PARTITION_KEY]
    )
    fun sortKey(): String?

    @CliOption(
        long = GLOBAL_INDEX_NAME,
        short = 'i',
        requires = [PARTITION_KEY],
        precludes = [CONSISTENT_READ]
    )
    @Pattern(regexp = "[a-zA-Z_\\-.]*")
    @Size(min = 3, max = 255)
    fun globalIndexName(): String?

    @CliOption(
        long = LIMIT,
        short = 'l'
    )
    @Positive
    fun limit(): Int?

    @CliOption(
        long = PROFILE,
        short = 'p'
    )
    fun profile(): String?

    @CliOption(
        long = REGION,
        short = 'R'
    )
    @Pattern(regexp = "[a-z0-9\\-]*")
    @Size(min = 3, max = 30)
    fun region(): String?

    @CliOption(
        long = ENDPOINT_URL,
        short = 'e'
    )
    fun endpointUrl(): String?

    @CliOption(
        long = COMPACT
    )
    fun compact(): Boolean

    @CliOption(
        long = START_KEY,
        short = 'k'
    )
    fun startKey(): String?

    @CliOption(
        long = REARRANGE_ATTRIBUTES,
        short = 'g'
    )
    fun rearrangeAttributes(): Boolean

    @CliOption(
        long = CONTENT_ONLY,
        short = 'C'
    )
    fun contentOnly(): Boolean

    @CliOption(
        long = SCAN_LIMIT,
        short = 'L',
        precludes = [CONCURRENCY]
    )
    @Positive
    fun scanLimit(): Int?

    @CliOption(
        long = AGGREGATE,
        short = 'a'
    )
    fun aggregate(): String?

    @CliOption(
        long = PROJECTION_EXPRESSION,
        short = 's'
    )
    fun projectionExpression(): String?

    @CliOption(
        long = CONCURRENCY,
        short = 'c',
        default = "1"
    )
    @Min(1)
    @Max(999)
    fun concurrency(): Int

    @CliOption(
        long = CONSISTENT_READ
    )
    fun consistentRead(): Boolean

    @CliOption(
        long = MAX_HEAP_SIZE,
        short = 'h'
    )
    @Min(200)
    @Max(8000)
    fun maxHeapSize(): Int?

    @CliOption(
        long = EXPAND,
        short = 'x',
        requires = [GLOBAL_INDEX_NAME]
    )
    fun expand(): Boolean

    @CliOption(
        long = STREAM,
        precludes = [AGGREGATE, REDUCE]
    )
    fun stream(): Boolean

    @CliOption(
        long = REDUCE,
        short = 'r'
    )
    @Size(min = 2, max = 3)
    fun reduce(): Array<String>?

}

private const val TABLE_NAME = "from"
private const val TRANSFORM = "transform"
private const val WHERE = "where"
private const val PRETRANSFORM = "pretransform"
private const val PARTITION_KEY = "partition-key"
private const val SORT_KEY = "sort-key"
private const val GLOBAL_INDEX_NAME = "global-index"
private const val LIMIT = "limit"
private const val PROFILE = "profile"
private const val REGION = "region"
private const val ENDPOINT_URL = "endpoint-url"
private const val COMPACT = "compact"
private const val START_KEY = "start-key"
private const val REARRANGE_ATTRIBUTES = "rearrange-attrs"
private const val CONTENT_ONLY = "content-only"
private const val SCAN_LIMIT = "scan-limit"
private const val AGGREGATE = "aggregate"
private const val PROJECTION_EXPRESSION = "select"
private const val CONCURRENCY = "concurrency"
private const val CONSISTENT_READ = "consistent-read"
private const val MAX_HEAP_SIZE = "max-heap-size"
private const val EXPAND = "expand"
private const val STREAM = "stream"
private const val REDUCE = "reduce"
