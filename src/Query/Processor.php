<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor as BaseProcessor;

class Processor extends BaseProcessor
{
    /**
     * Process the result of a select query.
     *
     * @param  Builder  $query
     * @param  array  $results
     * @return array
     */
    public function processSelect(Builder $query, $results)
    {
        $columns = [];
        $parsed = [];

        foreach ($results as $index => $row) {
            $values = array_map(fn($v) => $v['VarCharValue'] ?? null, $row['Data']);

            if ($index === 0) {
                $columns = $values;
            } else {
                $parsed[] = array_combine($columns, $values);
            }
        }

        return $parsed;
    }
}
