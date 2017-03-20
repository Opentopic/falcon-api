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
