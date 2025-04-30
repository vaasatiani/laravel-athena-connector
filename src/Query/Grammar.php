<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Grammars\MySqlGrammar;
use Illuminate\Database\ConnectionInterface;

class Grammar extends MySqlGrammar
{
    public function __construct(ConnectionInterface $connection)
    {
        parent::__construct($connection);
    }

    protected function compileLimit($query, $limit)
    {
        if (is_int($query->offset)) {
            return 'BETWEENLIMIT '.(int) $limit;
        }

        return parent::compileLimit($query, $limit);
    }

    protected function compileOffset($query, $offset)
    {
        return 'AND '.(int) $offset;
    }
}
