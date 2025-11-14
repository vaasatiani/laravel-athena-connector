<?php

namespace Vasatiani\Athena;

use Aws\Athena\AthenaClient;
use Exception;
use Illuminate\Database\MySqlConnection;
use Illuminate\Database\QueryException;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Vasatiani\Athena\Query\Grammar as QueryGrammar;
use Vasatiani\Athena\Query\Processor;
use Vasatiani\Athena\Schema\Builder;
use Vasatiani\Athena\Schema\Grammar as SchemaGrammar;

class Connection extends MySqlConnection
{
    protected ?AthenaClient $athenaClient = null;
    private ?string $localFilePath = null;

    public function __construct(array $config)
    {
        $this->config = config('athena');

        $this->config['s3output'] = 's3://' . $this->config['bucket'] . '/' . $this->config['outputfolder'];

        $this->database    = $this->config['database'];
        $this->tablePrefix = $this->config['prefix'] ?? '';

        $this->prepareAthenaClient();
        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }

    private function prepareAthenaClient(): void
    {
        if (is_null($this->athenaClient)) {
            $this->athenaClient = new AthenaClient([
                'version'     => 'latest',
                'region'      => $this->config['region'],
                'credentials' => $this->config['credentials'],
            ]);
        }
    }

    public function getDefaultPostProcessor(): Processor
    {
        return new Processor;
    }

    public function getDefaultQueryGrammar(): QueryGrammar
    {
        return new QueryGrammar($this);
    }

    public function useDefaultPostProcessor(): void
    {
        $this->postProcessor = $this->getDefaultPostProcessor();
    }

    public function useDefaultQueryGrammar(): void
    {
        $this->queryGrammar = $this->getDefaultQueryGrammar();
    }

    public function getSchemaBuilder(): Builder
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new Builder($this);
    }

    public function useDefaultSchemaGrammar(): void
    {
        $this->schemaGrammar = $this->getDefaultSchemaGrammar();
    }

    protected function getDefaultSchemaGrammar(): SchemaGrammar
    {
        return new SchemaGrammar;
    }

    /**
     * Safely escape a binding value for SQL query.
     *
     * @param mixed $value Value to escape
     * @return string Escaped value
     */
    protected function escapeBinding(mixed $value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        // Escape string values by replacing single quotes with two single quotes
        // This is the SQL standard way to escape quotes in string literals
        $escaped = str_replace("'", "''", (string) $value);
        return "'" . $escaped . "'";
    }

    /**
     * Prepare query by safely binding parameters.
     *
     * @param string $query SQL query with placeholders
     * @param array $bindings Array of values to bind
     * @return string Prepared query with bound values
     */
    protected function prepareQuery(string $query, array $bindings): string
    {
        foreach ($bindings as $bind) {
            $replacement = $this->escapeBinding($bind);
            $query = preg_replace('/\?/', $replacement, $query, 1);
        }

        if (stripos($query, 'BETWEENLIMIT') !== false) {
            if (stripos($query, 'ROW_NUMBER()') === false || stripos($query, ' rn ') === false) {
                throw new Exception(
                    "BETWEENLIMIT requires ROW_NUMBER() OVER(...) as rn alias. "
                    . "The query grammar should auto-generate this for LIMIT/OFFSET queries."
                );
            }

            $parts = preg_split("/BETWEENLIMIT/i", $query);
            preg_match_all('/\d+/', array_pop($parts), $matches);

            $perPage = ($matches[0][0] - 1);
            if ($perPage > 0) {
                $page  = ($matches[0][1] / $perPage) + 1;
                $from  = ($perPage * ($page - 1)) + 1;
                $to    = ($perPage * $page);
                $query = "SELECT * FROM ( " . Arr::first($parts) . " ) WHERE rn BETWEEN $from AND $to";
            }
        }

        return str_replace('`', '', $query);
    }

    /**
     * Execute an Athena query with concurrency control.
     *
     * Uses Redis locking to prevent exceeding AWS Athena's concurrent query limits.
     * Polls query status until completion or failure.
     *
     * @param string $query SQL query (will be modified with bound parameters)
     * @param array $bindings Parameter values to bind
     * @return array Query execution details from Athena
     * @throws Exception If query fails or is cancelled
     */
    protected function executeQuery(string &$query, array $bindings): array
    {
        $query = $this->prepareQuery($query, $bindings);

        $lockKey = config('athena.lock_key', 'athena:query:concurrency');
        $lockTimeout = config('athena.lock_timeout', 10); // lock held duration
        $lockWait = config('athena.lock_wait', 5); // max wait time
        $pollInterval = (int) config('athena.query_poll_interval', 1);

        return Cache::lock($lockKey, $lockTimeout)->block($lockWait, function () use (&$query, $pollInterval) {
            try {
                $result = $this->athenaClient->startQueryExecution([
                    'QueryString' => $query,
                    'QueryExecutionContext' => ['Database' => $this->config['database']],
                    'ResultConfiguration' => ['OutputLocation' => $this->config['s3output']],
                ]);
            } catch (\Throwable $e) {
                Log::error('Athena query execution failed to start', [
                    'error' => $e->getMessage(),
                    'query' => substr($query, 0, 500), // Log first 500 chars
                ]);
                throw new Exception("Failed to start Athena query execution: " . $e->getMessage(), 0, $e);
            }

            $queryId = $result->get('QueryExecutionId');

            $status = 'QUEUED';
            while (in_array($status, ['QUEUED', 'RUNNING'])) {
                sleep($pollInterval);
                $exec = $this->athenaClient->getQueryExecution(['QueryExecutionId' => $queryId]);
                $status = $exec['QueryExecution']['Status']['State'];

                if (in_array($status, ['FAILED', 'CANCELLED'])) {
                    $reason = $exec['QueryExecution']['Status']['StateChangeReason'] ?? 'unknown';
                    Log::error('Athena query execution failed', [
                        'query_id' => $queryId,
                        'status' => $status,
                        'reason' => $reason,
                        'query' => substr($query, 0, 500),
                    ]);
                    throw new Exception("Athena Query {$status}: {$reason}");
                }
            }

            return $exec->toArray();
        });
    }

    public function statement($query, $bindings = []): bool
    {
        if ($this->pretending()) return true;
        $start = microtime(true);
        $this->executeQuery($query, $bindings);
        $this->logQuery($query, [], $this->getElapsedTime($start));
        return true;
    }

    /**
     * Fetch paginated query results from Athena.
     *
     * Athena returns results in batches (default 500 rows) with NextToken for continuation.
     * This method aggregates all batches into a single array efficiently.
     *
     * @param string $queryId AWS Query Execution ID
     * @return array Processed result rows as associative arrays
     * @throws Exception If query execution failed
     */
    private function getDataWithQueryId(string $queryId): array
    {
        $maxResults = (int) config('athena.result_batch_size', 500);
        $response = $this->athenaClient->getQueryResults([
            'QueryExecutionId' => $queryId,
            'MaxResults' => $maxResults
        ])->toArray();
        $rows = $response['ResultSet']['Rows'];

        // Use array_push with spread operator for O(1) per-item append instead of O(n) array_merge
        while (!empty($response['NextToken'])) {
            $response = $this->athenaClient->getQueryResults([
                'QueryExecutionId' => $queryId,
                'NextToken' => $response['NextToken'],
                'MaxResults' => $maxResults
            ])->toArray();

            array_push($rows, ...$response['ResultSet']['Rows']);
        }

        return self::processResultRows($rows);
    }

    /**
     * Process Athena result rows into associative arrays.
     *
     * Athena returns results with column names in the first row and data in subsequent rows.
     * This method converts that format to Laravel-compatible associative arrays.
     *
     * @param array $rows Raw rows from Athena API
     * @return array Processed rows as associative arrays
     */
    private static function processResultRows(array $rows): array
    {
        $columns = [];
        $results = [];

        foreach ($rows as $index => $row) {
            $values = array_map(fn($col) => $col['VarCharValue'] ?? null, $row['Data']);
            if ($index === 0) {
                $columns = $values;
            } else {
                $results[] = array_combine($columns, $values);
            }
        }

        return $results;
    }

    /**
     * Execute a SELECT query with query caching support.
     *
     * Uses MD5 hash of prepared query to cache execution IDs, allowing result reuse.
     * Falls back to direct execution if cache table doesn't exist.
     *
     * @param string $query SQL query
     * @param array $bindings Parameter values to bind
     * @param bool $useReadPdo Unused parameter (kept for interface compatibility)
     * @return array Query results as associative arrays
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        $hash = md5($this->prepareQuery($query, $bindings));

        // Try to fetch from cache, if table exists
        try {
            $cached = \Vasatiani\Athena\AthenaQueryHash::where('query_hash', $hash)->first();

            if ($cached) {
                Log::debug('Using cached Athena query result', ['hash' => $hash]);
                return $this->getDataWithQueryId($cached->aws_return_id);
            }
        } catch (QueryException $e) {
            // Expected: cache table doesn't exist yet
            Log::debug('Query cache table not available, executing query directly', [
                'error' => $e->getMessage()
            ]);
        } catch (\Throwable $e) {
            // Unexpected error - log but don't fail the query
            Log::warning('Unexpected error checking query cache', [
                'error' => $e->getMessage(),
                'hash' => $hash
            ]);
        }

        if ($this->pretending()) return [];

        $start = microtime(true);
        $exec = $this->executeQuery($query, $bindings);

        // Try to store to cache, if table exists
        try {
            \Vasatiani\Athena\AthenaQueryHash::create([
                'query_hash' => $hash,
                'aws_return_id' => $exec['QueryExecution']['QueryExecutionId'],
            ]);
            Log::debug('Cached Athena query result', ['hash' => $hash]);
        } catch (QueryException $e) {
            // Expected: cache table doesn't exist
            Log::debug('Query cache table not available, skipping cache storage');
        } catch (\Throwable $e) {
            // Unexpected error - log but don't fail the query
            Log::warning('Unexpected error storing query cache', [
                'error' => $e->getMessage(),
                'hash' => $hash
            ]);
        }

        $results = $this->getDataWithQueryId($exec['QueryExecution']['QueryExecutionId']);
        $this->logQuery($query, [], $this->getElapsedTime($start));

        return $results;
    }

}
