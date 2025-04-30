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
     * @param string $column
     * @param string $from
     * @param string $to
     * @return static
     */
    public function whereDateBetween(string $column, string $from, string $to): static
    {
        return $this->whereBetween($column, [$from, $to]);
    }

    /**
     * Add a where clause for not null and not empty string.
     *
     * @param string $column
     * @return static
     */
    public function whereNotEmpty(string $column): static
    {
        return $this->whereNotNull($column)->where($column, '!=', '');
    }

    /**
     * Add a JSON key existence check.
     * This is useful for Athena where JSON fields may be queried using JSON_EXTRACT.
     *
     * @param string $column
     * @param string $key
     * @return static
     */
    public function whereJsonHasKey(string $column, string $key): static
    {
        return $this->whereRaw("json_extract($column, '$.\"$key\"') IS NOT NULL");
    }

    /**
     * Select only specific fields if none are already selected.
     *
     * @param array $columns
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
