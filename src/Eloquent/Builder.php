<?php

namespace Vasatiani\Athena\Eloquent;

use Illuminate\Database\Eloquent\Builder as BaseBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;

class Builder extends BaseBuilder
{
    public function __construct(QueryBuilder $query)
    {
        parent::__construct($query);
    }

    /**
     * Add a where between clause for date columns.
     *
     * Filters records where the column value falls within the specified date range (inclusive).
     *
     * @param string $column Column name containing date values
     * @param string $from Start date in YYYY-MM-DD format
     * @param string $to End date in YYYY-MM-DD format
     * @return static
     */
    public function whereDateBetween(string $column, string $from, string $to): static
    {
        return $this->whereBetween($column, [$from, $to]);
    }

    /**
     * Add a where clause for not null and not empty string.
     *
     * Filters records where the column is not NULL and not an empty string.
     *
     * @param string $column Column name to check
     * @return static
     */
    public function whereNotEmpty(string $column): static
    {
        return $this->whereNotNull($column)->where($column, '!=', '');
    }

    /**
     * Add a JSON key existence check.
     *
     * This is useful for Athena where JSON fields may be queried using JSON_EXTRACT.
     * Uses proper escaping to prevent SQL injection.
     *
     * @param string $column Column name containing JSON data
     * @param string $key JSON key to check for existence
     * @return static
     */
    public function whereJsonHasKey(string $column, string $key): static
    {
        // Escape key for safe JSON path usage
        $escapedKey = str_replace('"', '\\"', $key);
        return $this->whereRaw("json_extract({$column}, '$.\"' || ? || '\"') IS NOT NULL", [$escapedKey]);
    }

    /**
     * Select only specific fields if none are already selected.
     *
     * This method conditionally adds a select clause only if no columns
     * have been explicitly selected yet. Useful for setting default columns.
     *
     * @param array<string> $columns List of column names to select
     * @return static
     */
    public function selectIfEmpty(array $columns): static
    {
        if (empty($this->getQuery()->columns)) {
            $this->select($columns);
        }

        return $this;
    }
}
