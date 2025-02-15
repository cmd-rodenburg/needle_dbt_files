/*
 * Tests whether the [VALID_FROM] is smaller than [VALID_TO]
 */


{% test valid_from_and_to_dates(model) %}

    select VALID_FROM
    from {{ model }}
    WHERE IFNULL(VALID_FROM, '0000-12-31') > IFNULL(VALID_TO, '9999-12-31')

{% endtest %}