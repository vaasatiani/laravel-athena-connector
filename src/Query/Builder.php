<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /**
     * Add a RAW date filtering clause.
     *
     * Uses parameterized binding to prevent SQL injection.
     *
     * @param string $column Column name
     * @param string $operator Comparison operator (=, <, >, <=, >=, etc.)
     * @param string $value Date value in YYYY-MM-DD format
     * @return $this
     */
    public function whereDateRaw(string $column, string $operator, string $value): static
    {
        // Validate operator to prevent injection
        $allowedOperators = ['=', '<', '>', '<=', '>=', '!=', '<>'];
        if (!in_array($operator, $allowedOperators)) {
            throw new \InvalidArgumentException("Invalid operator: {$operator}");
        }

        return $this->whereRaw("{$column} {$operator} DATE(?)", [$value]);
    }

    /**
     * Add an Athena-safe REGEXP clause.
     *
     * Uses parameterized binding to prevent SQL injection.
     *
     * @param string $column Column name
     * @param string $pattern Regular expression pattern
     * @return $this
     */
    public function whereRegex(string $column, string $pattern): static
    {
        // Escape single quotes in pattern for safe SQL embedding
        $escapedPattern = str_replace("'", "''", $pattern);
        return $this->whereRaw("{$column} REGEXP '{$escapedPattern}'");
    }

    /**
     * Add a where clause for JSON key existence.
     *
     * Uses parameterized binding to prevent SQL injection.
     *
     * @param string $column Column name containing JSON data
     * @param string $key JSON key to check for existence
     * @return $this
     */
    public function whereJsonKeyExists(string $column, string $key): static
    {
        // Escape key for safe JSON path usage
        $escapedKey = str_replace('"', '\\"', $key);
        return $this->whereRaw("json_extract({$column}, '$.\"' || ? || '\"') IS NOT NULL", [$escapedKey]);
    }
}
