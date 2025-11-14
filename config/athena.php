<?php

return [

    /*
    |--------------------------------------------------------------------------
    | AWS Athena Configuration
    |--------------------------------------------------------------------------
    |
    | These options are used to configure the connection to AWS Athena.
    |
    */

    'credentials' => [
        'key'    => env('AWS_ATHENA_KEY', ''),
        'secret' => env('AWS_ATHENA_SECRET', ''),
    ],

    'region'        => env('AWS_ATHENA_REGION', 'eu-central-1'),
    'version'       => 'latest',
    'database'      => env('ATHENA_DB', ''),
    'prefix'        => env('ATHENA_TABLE_PREFIX', ''),
    'bucket'        => env('S3_ATHENA_BUCKET', ''),
    'outputfolder'  => env('ATHENA_OUTPUT_FOLDER', 'athena-output'),

    /*
    |--------------------------------------------------------------------------
    | S3 Output Location
    |--------------------------------------------------------------------------
    |
    | This is the S3 path where Athena stores query results.
    | It is auto-generated from bucket and outputfolder.
    |
    */

    's3output' => 's3://' . env('S3_ATHENA_BUCKET', '') . '/' . env('ATHENA_OUTPUT_FOLDER', 'athena-output'),

    /*
    |--------------------------------------------------------------------------
    | Query Concurrency Lock
    |--------------------------------------------------------------------------
    |
    | To avoid exceeding AWS Athena's 20 concurrent query limit,
    | a Redis lock is used to serialize queries.
    |
    */

    'lock_key'     => env('ATHENA_LOCK_KEY', 'athena:query:concurrency'),
    'lock_timeout' => env('ATHENA_LOCK_TIMEOUT', 10), // lock is held for 10 seconds
    'lock_wait'    => env('ATHENA_LOCK_WAIT', 5),      // wait up to 5 seconds for lock

    /*
    |--------------------------------------------------------------------------
    | Query Execution Settings
    |--------------------------------------------------------------------------
    |
    | Configuration for query execution behavior including polling intervals
    | and result batch sizes.
    |
    */

    'query_poll_interval' => env('ATHENA_QUERY_POLL_INTERVAL', 1), // seconds between status polls
    'result_batch_size'   => env('ATHENA_RESULT_BATCH_SIZE', 500), // rows per page in results

];
