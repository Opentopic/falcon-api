=================
Usage - filtering
=================

This page describes filtering params in GET requests to collection resources.

Simple filters
**************

All params that are not reserved keywords must be resource attribute or relation names with an optional operator,
joined using two underscores (`__`).

Examples:

```
some_text_column=exact_text_to_look_for
some_int_column__gte=5
```

Available operators:

* exact is ==
* notexact is !=
* gt is >
* lt is <
* gte is >=
* lte is <=
* range is >= and <=
* notrange is <= and >=
* in matches any value from a list
* notin
* contains matches a subset in a multivalue attribute
* notcontains
* match is a database specific match operator
* notmatch
* iexact is a case insensitive pattern match
* notiexact
* startswith is a prefix match
* notstartswith
* endswith is a suffix match
* notendswith
* hasall is a JSON attribute specific operator
* hasany
* haskey
* overlap matches any commmon value in a multivalue attribute
* istartswith
* notistartswith
* iendswith
* notiendswith
* isnull
* isnotnull
* year matches year part of a date like attribute
* month
* day
* func allows to call an arbitrary database funcion, using attribute name and provided value as arguments
* sfunc allows to call an arbitrary single argument database function, using only attribute name

Relation filters
****************

To filter by relation attributes, simply join the relation name and its attribute using two underscores:

```
relation_name__some_attribute=exact_value
```

Full text search
****************

This chapter describes the `q`, `q__and` and `q__or` params.

Advanced filters
****************

A `search` param provided an alternative syntax for filters by using JSON. This allows combining filters using `and`, `or` and `not` logical operators.

