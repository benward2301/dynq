# dynq

`dynq` is a command line analytic query tool for DynamoDB. It uses `jq` filters to target, transform and/or aggregate
items in a given table, and has a number of QoL features including automatic pagination, segmented scans
and index expansion.

![](docs/demo.gif)

## Options

#### `-f, --from`(table name)

*Required*

The name of the table containing the requested items; or, if you provide `--index`, the name of the
table to which that index belongs.

#### `-a, --aggregate` (jq filter)

`jq` filter to transform the complete query result set, after all transformations and exclusions have been
applied. The output of this filter is returned to the user via the `content` field.

For example, passing `--aggregate length` will result in a `content` value equal to the total hit count of the
query, assuming no other aggregation filters have been passed.

Incompatible with `--stream` and `--meta-only`.

#### `-c, --concurrency` (integer)

The number of coroutines to launch when reading from DynamoDB. Defaults to `1`.

For scan operations, this option is equivalent to the DynamoDB `--total-segments` option. The optimal number will
depend on the size and composition of the table, and can be gleaned by monitoring the log output and how long each
query takes. If no items are scanned from a segment, then this number should be lowered.

For non-scan operations, this option is only applicable if multiple keys have been passed.

Incompatible with `--scan-limit` and `--start-key`.

#### `-C, --content-only`

Return the unwrapped content of the query output.

Incompatible with `--meta-only`.

#### `--colorize`

Colorize JSON output. Enabled by default when destination is a TTY.

#### `--compact`

Compact instead of pretty-printed output.

#### `--consistent-read`

Guarantees that all writes completed before the query began will be processable.

#### `-e, --endpoint-url` (url)

Send DynamoDB requests to the given URL.

#### `-g, --rearrange-attrs`

Sort keys of objects on output.

Incompatible with `--meta-only`.

#### `-i, --index` (index name)

The name of a global secondary index to query.

Requires `--partition-key`.

Incompatible with `--consitent-read`.

#### `-I, --items-per-request` (integer)

The maximum number of items scanned per DynamoDB request.

#### `-k, --start-key` (jq filter)

`jq` filter producing the last evaluated key from a previous DynamoDB scan or query operation. When applicable,
`dynq` will return the last evaluated key of any such operations via the `meta.lastEvaluatedKey` field.

If a partition key has been passed via the `--partition-key` option, then this filter may only produce the sort key.

Incompatible with `--sort-key` and `--concurrency`.

#### `-l, --limit` (integer)

The maximum number of DynamoDB items to retain after selection.

Note that `meta.lastEvaluatedKey` will not be returned when this option is given.

#### `-L, --scan-limit` (integer)

The maximum number of DynamoDB items to scan across one or more requests.

Unlike `--limit`, `meta.lastEvaluatedKey` may be returned when this option is given.

#### `-M, --meta-only`

Return the unwrapped metadata of the query output. Can be used when only the total number of hits is needed.

Incompatible with `--content-only`, `--transform`, `--aggregate`, `--prune`, `--reduce`, `--rearrange-attrs`
and `stream`.

#### `--max-heap-size` (integer)

Heap memory limit in megabytes. The query will terminate and its results will be returned when this threshold is
approached.

#### `--monochrome`

Do not colorize JSON output. Enabled by default if destination is not a TTY.

Incompatible with `--colorize`.

#### `--partition-key, -P` (jq filter)

`jq` filter producing one or more partition keys.

The output must be an object containing a single key (the partition key attribute name), the value of which must be a
string, number, or array of either. An array may be used to query multiple partition keys in a single operation.

#### `-p, --profile` (aws profile)

Profile to use from your AWS credentials file.

#### `-Q, --request-limit` (integer)

The maximum number of requests to send to DynamoDB per coroutine.

#### `q, --quiet`

Only write to stderr when an error is encountered.

#### `-r, --reduce` (starting value) (jq filter)

Reduce items using the given starting value and `jq` filter, with items assigned to `$item`.

Equivalent to `jq` `reduce .[] as $item (<starting value>; <jq filter>)`

Incompatible with `--stream`, `--prune` and `--meta-only`.

#### `-R, --region` (aws region)

The AWS region to use. Overrides config/env settings.

#### `-s, --select` (projection expression)

A comma-separated set of attribute names to retrieve. Equivalent to the DynamoDB `--projection-expression` option.

Can improve performance of queries.

#### `-S, --sort-key` (jq filter)

`jq` filter producing one or more discrete sort keys, or a sort key range.

The output must be an object containing a single key (the partition key attribute name).

To target specific items, the value must be a string, number or array of either, as with the `--partition-key` option,
or a nested object with a single key `eq` or `equals` and aforementioned value.

To target a range of items, the value must be an object with one of the following keys:

- `lt` or `less_than`
- `lte` or `less_than_or_equals`
- `gt` or `greater_than`
- `gte` or `greater_than_or_equals`
- `bw` or `begins_with`

The nested value must be a string or number. A lower bound `gt(e)` and upper bound `lt(e)` may both be
passed.

Requires `--partition-key`.

Incompatible with `--start-key`.

#### `-s, --stream`

Incrementally write items to stdout.

Incompatible with `--aggregate`, `--reduce` and `--meta-only`.

#### `-T, --pretransform` (jq filter)

`jq` filter to transform each individual item, as returned from DynamoDB. Executes before the `--where` selection
filter.

#### `-t, --transform` (jq filter)

`jq` filter to transform individual items. Executes after the `--where` selection filter.

Incompatible with `--meta-only`.

#### `-u, --prune` (jq filter)

`jq` filter to transform the cumulative result set, executed after each request to DynamoDB. Must return an array.

This filter can be used to find the least/greatest *n* values according to some comparator, or find distinct values.

Where possible, it should be used over `--aggregate` for high-volume queries to reduce memory usage.

Incompatible with `--meta-only`

#### `-w, --where` (jq filter)

`jq` predicate filter to select/discard items. Equivalent to `jq` `select(f)` function.

#### `-x, --expand`

Retrieve non-projected attributes from the primary table when querying a global index.

Requires `--partition-key` and `--index`.
