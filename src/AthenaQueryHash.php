<?php

namespace Vasatiani\Athena;

use Illuminate\Database\Eloquent\Model;

class AthenaQueryHash extends Model
{
    protected $table = 'athena_query_hashes';

    protected $fillable = [
        'query_hash',
        'aws_return_id',
    ];

    public $timestamps = true;

    /**
     * Disable auto incrementing ID (optional)
     */
    public $incrementing = false;

    protected $primaryKey = 'query_hash';
    protected $keyType = 'string';
}
