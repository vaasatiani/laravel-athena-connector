<?php

namespace Vasatiani\Athena\Eloquent;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;

class Collection extends EloquentCollection
{
    /**
     * Return only the values of a specific column, excluding nulls.
     *
     * @param string $column
     * @return \Illuminate\Support\Collection
     */
    public function pluckNonNull(string $column)
    {
        return $this->pluck($column)->filter();
    }

    /**
     * Return a grouped collection by a given field.
     *
     * @param string $key
     * @return \Illuminate\Support\Collection
     */
    public function groupByKey(string $key)
    {
        return $this->groupBy(function ($item) use ($key) {
            return data_get($item, $key);
        });
    }

    /**
     * Convert collection to array with only selected fields.
     *
     * @param array $fields
     * @return array
     */
    public function toArrayWithFields(array $fields)
    {
        return $this->map(function ($item) use ($fields) {
            return collect($item)->only($fields)->toArray();
        })->toArray();
    }
}
