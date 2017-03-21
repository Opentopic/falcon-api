=========
Filtering
=========

This page describes filtering params in GET requests to collection resources.

Simple filters
**************

All params that are not reserved keywords must be resource attribute or relation names with an optional operator,
joined using two underscores (`__`).

Examples ::

    some_text_column=exact_text_to_look_for
    some_int_column__gte=5

Available operators:

* exact is ==
* notexact is !=
* gt is >
* lt is <
* gte is >=
* lte is <=
* range is >= and <= (BETWEEN)
* notrange is <= and >=
* in matches any value from a list
* notin
* contains matches a subset in a multivalue attribute (the `&&` operator)
* notcontains
* match is a database specific match operator
* notmatch
* iexact is a case insensitive pattern match (ILIKE)
* notiexact
* startswith is a prefix match
* notstartswith
* endswith is a suffix match
* notendswith
* hasall is a JSON and HSTORE attribute specific operator
* hasany
* haskey
* overlap matches any commmon value in a multivalue attribute
* istartswith
* notistartswith
* iendswith
* notiendswith
* isnull
* isnotnull
* year matches year part of a date like attribute (EXTRACT function)
* month
* day
* sfunc allows to call an arbitrary single argument database function, using only attribute name
* func allows to call an arbitrary database funcion, using attribute name and provided value as arguments

Some operator behavior, like `contains`, depends on column type.

Example ::

    name__iexact=John&categories__sfunc__jsonb_array_length__gt=3

The example above uses the `sfunc` operator which applies the `jsonb_to_int_array` SQL function
to the `categories` attribute and compares the result with 3.

Two argument functions must return a boolean and are most useful
when you want to use a special operator that's not supported, so you call it's function instead.

Relation filters
****************

To filter by relation attributes, simply join the relation name and its attribute using two underscores::

    relation_name__some_attribute=exact_value

Advanced filters
****************

A `search` param provided an alternative syntax for filters by using JSON instead of plain key-value pairs.
This allows combining filters using `and`, `or` and `not` logical operators.
Value for a logical operator must be either a:

* dict, where keys are other operator names
* list of dicts


Example ::

    {
      "and": [
        {"category_id__in": [1, 2, 3, 4]},
        {"not": {"name__startswith":"Bollywood"}}
      ]
    }

Note: advanced filters will be joined with simple filters using `and`.
To improve code readability try to avoid mixing those two formats.

Full text search
****************

A resource might optionally support full text search. Use the `q` param to search for a phrase
on attributes defined in the resource or choose specific attributes by appending them after two underscores (`__`).

Special variants:

* `q__and` - all phrase parts must be present in results
* `q__or` - any of phrase parts must be present in results
