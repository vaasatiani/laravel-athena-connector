<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\ConnectionInterface;

/**
 * Query grammar for AWS Athena.
 *
 * Extends MySQL grammar and overrides LIMIT/OFFSET compilation to use
 * a custom BETWEENLIMIT token which is later transformed into a ROW_NUMBER()
 * based pagination query in the Connection class.
 */
class Grammar extends MySqlGrammar
{
    public function __construct(ConnectionInterface $connection)
    {
        parent::__construct($connection);
    }

    /**
     * Compile the LIMIT clause for Athena queries.
     *
     * When an OFFSET is present, uses a custom BETWEENLIMIT token that will be
     * transformed into a ROW_NUMBER() window function by the Connection class,
     * as Athena doesn't support LIMIT/OFFSET directly.
     *
     * @param \Illuminate\Database\Query\Builder $query Query builder instance
     * @param int $limit Maximum number of rows to return
     * @return string Compiled LIMIT clause or BETWEENLIMIT token
     */
    protected function compileLimit($query, $limit)
    {
        if (is_int($query->offset)) {
            return 'BETWEENLIMIT '.(int) $limit;
        }

        return parent::compileLimit($query, $limit);
    }

    /**
     * Compile the OFFSET clause for Athena queries.
     *
     * Returns a special 'AND' token that works with BETWEENLIMIT to form
     * a complete pagination expression (e.g., "BETWEENLIMIT 10 AND 20").
     *
     * @param \Illuminate\Database\Query\Builder $query Query builder instance
     * @param int $offset Number of rows to skip
     * @return string 'AND' token with offset value
     */
    protected function compileOffset($query, $offset)
    {
        return 'AND '.(int) $offset;
    }
}
