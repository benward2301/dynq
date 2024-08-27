package dynq.cli.command

interface Command {

    fun getMutuallyExclusiveOptions(): Collection<Pair<Function<*>, Function<*>>> {
        return emptyList()
    }

    fun getOptionDependencies(): Collection<Pair<Function<*>, Function<*>>> {
        return emptyList()
    }

}
