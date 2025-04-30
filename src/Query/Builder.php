<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /**
     * Add a RAW date filtering clause.
     *
     * @param string $column
     * @param string $operator
     * @param string $value
     * @return $this
     */
    public function whereDateRaw(string $column, string $operator, string $value): static
    {
        return $this->whereRaw("$column $operator DATE('$value')");
    }

    /**
     * Add a Athena-safe REGEXP clause
     *
     * @param string $column
     * @param string $pattern
     * @return $this
     */
    public function whereRegex(string $column, string $pattern): static
    {
        return $this->whereRaw("$column REGEXP '$pattern'");
    }

    /**
     * Add a where clause for JSON key existence.
     *
     * @param string $column
     * @param string $key
     * @return $this
     */
    public function whereJsonKeyExists(string $column, string $key): static
    {
        return $this->whereRaw("json_extract($column, '$.\"$key\"') IS NOT NULL");
    }
}
