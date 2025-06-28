package dynq.cli.command

import dynq.cli.anno.CliCommand
import dynq.cli.anno.CliOption
import dynq.cli.anno.constraints.*
import dynq.cli.command.option.JQ_FILTER_ARG
import dynq.cli.command.option.JQ_REDUCE_INIT_ARG
import dynq.cli.command.option.JQ_REDUCE_ITEM_VAR

@CliCommand(root = true)
interface ReadCommand : Command {

    @CliOption(
        long = TABLE_NAME,
        short = 'f',
        args = ["table-name"]
    )
    @Pattern(regexp = RESOURCE_NAME_PATTERN)
    @Size(min = 3, max = 255)
    fun tableName(): String

    @CliOption(
        long = TRANSFORM,
        short = 't',
        args = [JQ_FILTER_ARG],
        desc = "item transformation filter;\nequivalent to jq map(f) function"
    )
    fun transform(): String?

    @CliOption(
        long = WHERE,
        short = 'w',
        args = [JQ_FILTER_ARG],
        desc = "item selection filter;\nequivalent to jq select(f) function"
    )
    fun where(): String?

    @CliOption(
        long = PRETRANSFORM,
        short = 'T',
        args = [JQ_FILTER_ARG],
        desc = "same as --$TRANSFORM, but executed before --$WHERE"
    )
    fun pretransform(): String?

    @CliOption(
        long = KEY,
        short = 'K',
        precludes = [START_KEY, PARTITION_KEY, SORT_KEY],
        args = [JQ_FILTER_ARG],
        desc = "(composite) key filter"
    )
    fun key(): String?

    @CliOption(
        long = PARTITION_KEY,
        short = 'P',
        args = [JQ_FILTER_ARG],
        desc = "partition key filter"
    )
    fun partitionKey(): String?

    @CliOption(
        long = SORT_KEY,
        short = 'S',
        requires = [PARTITION_KEY],
        precludes = [START_KEY],
        args = [JQ_FILTER_ARG],
        desc = "sort key filter"
    )
    fun sortKey(): String?

    @CliOption(
        long = INDEX_NAME,
        short = 'i',
        precludes = [CONSISTENT_READ],
        args = ["index-name"]
    )
    @Pattern(regexp = RESOURCE_NAME_PATTERN)
    @Size(min = 3, max = 255)
    fun indexName(): String?

    @CliOption(
        long = LIMIT,
        short = 'l',
        desc = "maximum number of items to return"
    )
    @Positive
    fun limit(): Int?

    @CliOption(
        long = PROFILE,
        short = 'p',
        args = ["aws-profile"]
    )
    fun profile(): String?

    @CliOption(
        long = REGION,
        short = 'R',
        args = ["aws-region"]
    )
    @Pattern(regexp = RESOURCE_NAME_PATTERN)
    @Size(min = 3, max = 30)
    fun region(): String?

    @CliOption(
        long = ENDPOINT_URL,
        short = 'E',
        args = ["url"]
    )
    fun endpointUrl(): String?

    @CliOption(
        long = COMPACT,
        short = 'm',
        desc = "compact instead of pretty-printed output"
    )
    fun compact(): Boolean

    @CliOption(
        long = START_KEY,
        short = 'k',
        args = [JQ_FILTER_ARG],
        desc = "last evaluated key from a previous scan or query operation",
        precludes = [CONCURRENCY]
    )
    fun startKey(): String?

    @CliOption(
        long = REARRANGE_KEYS,
        short = 'g',
        desc = "sort keys of objects on output"
    )
    fun rearrangeKeys(): Boolean

    @CliOption(
        long = CONTENT_ONLY
    )
    fun contentOnly(): Boolean

    @CliOption(
        long = SCAN_LIMIT,
        short = 'L',
        precludes = [CONCURRENCY],
        desc = "maximum number of items to scan"
    )
    @Positive
    fun scanLimit(): Int?

    @CliOption(
        long = AGGREGATE,
        short = 'a',
        args = [JQ_FILTER_ARG],
        desc = "aggregation filter;\ntakes complete result array as input"
    )
    fun aggregate(): String?

    @CliOption(
        long = PROJECTION_EXPRESSION,
        short = 's',
        args = ["projection-expr"],
        desc = "comma-separated set of attribute names to retrieve"
    )
    fun projectionExpression(): String?

    @CliOption(
        long = CONCURRENCY,
        short = 'c',
        default = "1",
        desc = "number of coroutines to launch"
    )
    @Min(1)
    @Max(999)
    fun concurrency(): Int

    @CliOption(
        long = CONSISTENT_READ,
        desc = "strongly consistent instead of eventually consistent read"
    )
    fun consistentRead(): Boolean

    @CliOption(
        long = EXPAND,
        short = 'x',
        requires = [INDEX_NAME],
        desc = "retrieve non-projected attributes when --$INDEX_NAME is given"
    )
    fun expand(): Boolean

    @CliOption(
        long = STREAM,
        short = 'e',
        precludes = [AGGREGATE, REDUCE, PRUNE],
        desc = "incrementally write items to stdout"
    )
    fun stream(): Boolean

    @CliOption(
        long = REDUCE,
        short = 'r',
        args = [JQ_REDUCE_INIT_ARG, JQ_FILTER_ARG],
        desc = "reduce items using starting value <$JQ_REDUCE_INIT_ARG> and update <$JQ_FILTER_ARG>, " +
                "with items assigned to $JQ_REDUCE_ITEM_VAR;\n" +
                "equivalent to jq reduce .[] as $JQ_REDUCE_ITEM_VAR (<$JQ_REDUCE_INIT_ARG>; <$JQ_FILTER_ARG>)"
    )
    @Size(min = 2, max = 2)
    fun reduce(): Array<String>?

    @CliOption(
        long = REQUEST_LIMIT,
        short = 'Q',
        desc = "maximum number of DynamoDB requests per coroutine"
    )
    @Positive
    fun requestLimit(): Int?

    @CliOption(
        long = ITEMS_PER_REQUEST,
        short = 'I',
        desc = "maximum number of items per DynamoDB request"
    )
    @Positive
    fun itemsPerRequest(): Int?

    @CliOption(
        long = METADATA_ONLY,
        precludes = [CONTENT_ONLY, TRANSFORM, AGGREGATE, PRUNE, REDUCE, REARRANGE_KEYS, STREAM]
    )
    fun metadataOnly(): Boolean

    @CliOption(
        long = PRUNE,
        short = 'u',
        precludes = [REDUCE, STREAM],
        args = [JQ_FILTER_ARG],
        desc = "aggregation filter executed once per DynamoDB request;\n" +
                "takes partial result array as input and must return an array"
    )
    fun prune(): String?

}

private const val TABLE_NAME = "from"
private const val TRANSFORM = "transform"
private const val WHERE = "where"
private const val PRETRANSFORM = "pretransform"
private const val KEY = "key"
private const val PARTITION_KEY = "partition-key"
private const val SORT_KEY = "sort-key"
private const val INDEX_NAME = "index"
private const val LIMIT = "limit"
private const val PROFILE = "profile"
private const val REGION = "region"
private const val ENDPOINT_URL = "endpoint-url"
private const val COMPACT = "compact"
private const val START_KEY = "start-key"
private const val REARRANGE_KEYS = "rearrange-keys"
private const val CONTENT_ONLY = "content-only"
private const val SCAN_LIMIT = "scan-limit"
private const val AGGREGATE = "aggregate"
private const val PROJECTION_EXPRESSION = "select"
private const val CONCURRENCY = "concurrency"
private const val CONSISTENT_READ = "consistent-read"
private const val EXPAND = "expand"
private const val STREAM = "stream"
private const val REDUCE = "reduce"
private const val REQUEST_LIMIT = "request-limit"
private const val ITEMS_PER_REQUEST = "items-per-request"
private const val METADATA_ONLY = "meta-only"
private const val PRUNE = "prune"

private const val RESOURCE_NAME_PATTERN = "[\\w\\-.]*"
