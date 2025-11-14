<?php

namespace Vasatiani\Athena\Schema;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\MySqlGrammar;
use Illuminate\Support\Fluent;

/**
 * Schema grammar for AWS Athena.
 *
 * Generates CREATE EXTERNAL TABLE statements compatible with Athena's DDL syntax.
 * Strips unsupported MySQL modifiers like UNSIGNED, CHARSET, COLLATE, and COMMENT.
 */
class Grammar extends MySqlGrammar
{
    /**
     * Compile a CREATE EXTERNAL TABLE command for Athena.
     *
     * Generates Athena-compatible DDL with column definitions, storage format,
     * S3 location, and table properties. Uses configuration values for customization.
     *
     * @param Blueprint $blueprint Table schema definition
     * @param Fluent $command Create command details
     * @return string Compiled CREATE EXTERNAL TABLE statement
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
     * Strip UNSIGNED modifier (not supported by Athena).
     *
     * @param Blueprint $blueprint Table schema definition
     * @param Fluent $column Column definition
     * @return string Empty string (modifier removed)
     */
    protected function modifyUnsigned(Blueprint $blueprint, Fluent $column)
    {
        return '';
    }

    /**
     * Strip CHARSET modifier (not supported by Athena).
     *
     * @param Blueprint $blueprint Table schema definition
     * @param Fluent $column Column definition
     * @return string Empty string (modifier removed)
     */
    protected function modifyCharset(Blueprint $blueprint, Fluent $column)
    {
        return '';
    }

    /**
     * Strip COLLATE modifier (not supported by Athena).
     *
     * @param Blueprint $blueprint Table schema definition
     * @param Fluent $column Column definition
     * @return string Empty string (modifier removed)
     */
    protected function modifyCollate(Blueprint $blueprint, Fluent $column)
    {
        return '';
    }

    /**
     * Strip COMMENT modifier (not supported by Athena).
     *
     * @param Blueprint $blueprint Table schema definition
     * @param Fluent $column Column definition
     * @return string Empty string (modifier removed)
     */
    protected function modifyComment(Blueprint $blueprint, Fluent $column)
    {
        return '';
    }
}
