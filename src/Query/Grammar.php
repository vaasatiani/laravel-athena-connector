<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\Query\Builder;

class Grammar extends MySqlGrammar
{
    /**
     * Compile a custom LIMIT clause for Athena (Presto-compatible).
     *
     * @param  Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        if (is_int($query->offset)) {
            return 'BETWEENLIMIT ' . (int) $limit;
        }

        return parent::compileLimit($query, $limit);
    }

    /**
     * Athena-specific OFFSET simulation marker.
     *
     * @param  Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        return 'AND ' . (int) $offset;
    }
}
