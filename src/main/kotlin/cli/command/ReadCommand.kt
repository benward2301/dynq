package dynq.cli.command

import dynq.cli.anno.CliCommand
import dynq.cli.anno.CliOption

@CliCommand(root = true)
interface ReadCommand : Command {

    @CliOption(
        short = 'f',
        long = "from",
    )
    fun tableName(): String

    @CliOption(
        short = 't',
        long = "transform",
    )
    fun transform(): String?

    @CliOption(
        short = 'w',
        long = "where",
    )
    fun where(): String?

    @CliOption(
        long = "pretransform",
        short = 'T'
    )
    fun pretransform(): String?

    @CliOption(
        short = 'P',
        long = "partition-key"
    )
    fun partitionKey(): String?

    @CliOption(
        short = 'S',
        long = "sort-key"
    )
    fun sortKey(): String?

    @CliOption(
        short = 'i',
        long = "global-index"
    )
    fun globalIndexName(): String?

    @CliOption(
        short = 'l',
        long = "limit",
    )
    fun limit(): Int?

    @CliOption(
        short = 'p',
        long = "profile",
    )
    fun profile(): String?

    @CliOption(
        short = 'r',
        long = "region",
    )
    fun region(): String?

    @CliOption(
        short = 'e',
        long = "endpoint-url",
    )
    fun endpointUrl(): String?

    @CliOption(
        long = "colorize"
    )
    fun colorize(): Boolean

    @CliOption(
        long = "monochrome"
    )
    fun monochrome(): Boolean

    @CliOption(
        long = "compact"
    )
    fun compact(): Boolean

    @CliOption(
        short = 'k',
        long = "start-key"
    )
    fun startKey(): String?

    @CliOption(
        short = 'g',
        long = "rearrange-attrs"
    )
    fun rearrangeAttributes(): Boolean

    @CliOption(
        short = 'C',
        long = "content-only"
    )
    fun contentOnly(): Boolean

    @CliOption(
        short = 'L',
        long = "scan-limit"
    )
    fun scanLimit(): Int?

    @CliOption(
        short = 'a',
        long = "aggregate"
    )
    fun aggregate(): String?

    @CliOption(
        short = 's',
        long = "select"
    )
    fun projectionExpression(): String?

    @CliOption(
        short = 'c',
        long = "concurrency"
    )
    fun concurrency(): Int {
        return 1
    }

    @CliOption(
        long = "consistent-read"
    )
    fun consistentRead(): Boolean

    @CliOption(
        short = 'h',
        long = "max-heap-size"
    )
    fun maxHeapSize(): Int?

    @CliOption(
        short = 'x',
        long = "expand"
    )
    fun expand(): Boolean

    @CliOption(
        long = "stream"
    )
    fun stream(): Boolean
//
//    @CliOption(
//        short = 'M',
//        long = "metadata-only"
//    )
//    fun metadataOnly(): Boolean
//
//    @CliOption(
//        short = 'R',
//        long = "reduce"
//    )
//    fun reduce(): List<String>?

    override fun getMutuallyExclusiveOptions(): Collection<Pair<Function<*>, Function<*>>> {
        return listOf(
            Pair(::colorize, ::monochrome),
            Pair(::scanLimit, ::partitionKey),
            Pair(::scanLimit, ::sortKey),
            Pair(::scanLimit, ::concurrency),
            Pair(::globalIndexName, ::consistentRead),
//            Pair(::contentOnly, ::metadataOnly)
//            Pair(::stream, ::aggregate)
//            Pair(::metadataOnly, ::contentOnly)
        )
    }

    override fun getOptionDependencies(): Collection<Pair<Function<*>, Function<*>>> {
        return listOf(
            Pair(::globalIndexName, ::partitionKey),
            Pair(::sortKey, ::partitionKey),
//            Pair(::expandKeys, ::globalIndexName),
        )
    }

}
