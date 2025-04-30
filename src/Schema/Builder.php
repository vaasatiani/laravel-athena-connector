<?php

namespace Vasatiani\Athena\Schema;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Closure;

class Builder extends \Illuminate\Database\Schema\Builder
{
    /**
     * Run a callback with a new blueprint instance.
     *
     * @param string  $table
     * @param Closure $callback
     * @return void
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        return new Blueprint($table, $callback);
    }
}
