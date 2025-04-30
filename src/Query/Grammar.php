<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\Query\Builder;

class Grammar extends MySqlGrammar
{
    /**
     * Compile a custom LIMIT clause for Athena (Presto-compatible).
     * Instead of using native LIMIT/OFFSET, we use a marker string
     * for ROW_NUMBER() simulation in Connection::prepareQuery().
     *
     * @param  Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        // Use special marker understood by Connection::prepareQuery()
        if (is_int($query->offset)) {
            return 'BETWEENLIMIT '.(int) $limit;
        }

        // fallback — probably unnecessary for Athena, but safe for compatibility
        return parent::compileLimit($query, $limit);
    }

    /**
     * This just appends the offset value in a way that's later handled in prepareQuery.
     * It does NOT produce valid SQL and must only be used with compileLimit override.
     *
     * @param  Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        return 'AND '.(int) $offset;
    }
}
