package dynq.cli.command

import dynq.cli.anno.CliCommand
import dynq.cli.anno.CliOption
import jakarta.validation.constraints.*

@CliCommand(root = true)
interface ReadCommand : Command {

    @CliOption(
        short = 'f',
        long = "from",
    )
    @Pattern(regexp = "[a-zA-Z_\\-.]*")
    @Size(min = 3, max = 255)
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
    @Pattern(regexp = "[a-zA-Z_\\-.]*")
    @Size(min = 3, max = 255)
    fun globalIndexName(): String?

    @CliOption(
        short = 'l',
        long = "limit",
    )
    @Positive
    fun limit(): Int?

    @CliOption(
        short = 'p',
        long = "profile",
    )
    fun profile(): String?

    @CliOption(
        short = 'R',
        long = "region",
    )
    @Pattern(regexp = "[a-z0-9\\-]*")
    @Size(min = 3, max = 30)
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
    @Positive
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
    @Min(1)
    @Max(999)
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
    @Min(200)
    @Max(8000)
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

    @CliOption(
        short = 'r',
        long = "reduce"
    )
    @Size(min = 2, max = 3)
    fun reduce(): Array<String>?

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
