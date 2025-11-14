<?php

namespace Vasatiani\Athena\Eloquent;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Support\Collection as BaseCollection;

class Collection extends EloquentCollection
{
    /**
     * Return only the values of a specific column, excluding nulls.
     *
     * @param string $column Column name to pluck values from
     * @return BaseCollection Collection of non-null values
     */
    public function pluckNonNull(string $column): BaseCollection
    {
        return $this->pluck($column)->filter();
    }

    /**
     * Return a grouped collection by a given field.
     *
     * Supports nested field access using dot notation (e.g., 'user.name').
     *
     * @param string $key Field name to group by (supports dot notation)
     * @return BaseCollection Grouped collection
     */
    public function groupByKey(string $key): BaseCollection
    {
        return $this->groupBy(function ($item) use ($key) {
            return data_get($item, $key);
        });
    }

    /**
     * Convert collection to array with only selected fields.
     *
     * Extracts only specified fields from each item in the collection.
     *
     * @param array<string> $fields List of field names to include
     * @return array Array of items with only selected fields
     */
    public function toArrayWithFields(array $fields): array
    {
        return $this->map(function ($item) use ($fields) {
            return collect($item)->only($fields)->toArray();
        })->toArray();
    }
}
