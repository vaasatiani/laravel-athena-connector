<?php

namespace Vasatiani\Athena\Schema;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\MySqlGrammar;
use Illuminate\Support\Fluent;

class Grammar extends MySqlGrammar
{
    /**
     * Compile a create table command for Athena.
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command)
    {
        $columns = implode(', ', $this->getColumns($blueprint));
        $table   = $this->wrapTable($blueprint);

        $location      = config('athena.s3output', 's3://example-bucket/fallback/');
        $storedAs      = config('athena.stored_as', 'PARQUET');
        $tblProperties = config('athena.table_properties', [
            'has_encrypted_data' => 'false'
        ]);

        $properties = collect($tblProperties)->map(function ($value, $key) {
            return "'$key'='$value'";
        })->implode(', ');

        return "CREATE EXTERNAL TABLE {$table} ({$columns})\n" .
            "STORED AS {$storedAs}\n" .
            "LOCATION '{$location}'\n" .
            "TBLPROPERTIES ({$properties})";
    }

    /**
     * Strip unsupported modifiers from Athena columns.
     */
    protected function modifyUnsigned(Blueprint $blueprint, Fluent $column) { return ''; }
    protected function modifyCharset(Blueprint $blueprint, Fluent $column) { return ''; }
    protected function modifyCollate(Blueprint $blueprint, Fluent $column) { return ''; }
    protected function modifyComment(Blueprint $blueprint, Fluent $column) { return ''; }
}
