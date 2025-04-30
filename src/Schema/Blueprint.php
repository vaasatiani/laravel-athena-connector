<?php

namespace Vasatiani\Athena\Schema;

use Illuminate\Database\Schema\Blueprint as BaseBlueprint;

class Blueprint extends BaseBlueprint
{
    /**
     * Define a timestamp column with Athena-friendly settings.
     *
     * @param string $column
     * @return \Illuminate\Support\Fluent
     */
    public function athenaTimestamp(string $column)
    {
        return $this->addColumn('timestamp', $column, ['nullable' => true]);
    }
}
