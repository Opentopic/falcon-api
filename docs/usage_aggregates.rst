==========
Aggregates
==========

This page describes aggregate params in GET requests to collection resources.

The simplest and most commonly used aggregate is a total count of results.
To request it, set the `total_count` param to true. The value is returned in `total` key and `x-api-total` HTTP header.

The `totals` param allows requesting other aggregates. It can be a list of comma separated expressions,
same as for filters, or a JSON object.

If the latter, keys should be aggregate function names and values are attribute names.
Only the `count` aggregate doesn't require any attribute names.

Results are added to the response using the aggregate function name as key with the `total_` prefix, for example `total_sum`.

There are few special keys recognized:

* `group_by` - a list of attributes to group requested metrics by; order of attributes is retained in the results; values in groups are in descending order
* `group_limit` - makes each group have at most N items

Example ::

    {
      "sum": {"value": "created_at__sfunc__date_trunc_month"},
      "group_by": "category_id",
      "group_limit": 5,
    }

The example above uses a single argument SQL function called `date_trunc_month` to truncate the date to full month.

Note: because it's only possible to use single argument functions here, the `date_trunc_month` is actually a custom function
defined as a wrapper to `date_trunc` setting the second argument to `month`.
