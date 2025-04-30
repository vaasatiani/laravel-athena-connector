<?php

namespace Vasatiani\Athena;

use Aws\Athena\AthenaClient;
use Exception;
use Illuminate\Database\MySqlConnection;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Cache;
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
        return $this->withTablePrefix(new QueryGrammar);
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

    protected function prepareQuery(string $query, array $bindings): string
    {
        foreach ($bindings as $bind) {
            $replacement = is_numeric($bind) ? $bind : "'" . addslashes($bind) . "'";
            $query = preg_replace('/\?/', $replacement, $query, 1);
        }

        if (stripos($query, 'BETWEENLIMIT') !== false) {
            if (!stripos($query, 'ROW_NUMBER()') || !stripos($query, ' rn ')) {
                throw new Exception("Missing ROW_NUMBER() OVER(...) as rn for LIMIT simulation");
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

    protected function executeQuery(string &$query, array $bindings): array
    {
        $query = $this->prepareQuery($query, $bindings);

        $lockKey = config('athena.lock_key', 'athena:query:concurrency');
        $lockTimeout = config('athena.lock_timeout', 10); // lock held duration
        $lockWait = config('athena.lock_wait', 5); // max wait time

        return Cache::lock($lockKey, $lockTimeout)->block($lockWait, function () use (&$query) {
            $result = $this->athenaClient->startQueryExecution([
                'QueryString' => $query,
                'QueryExecutionContext' => ['Database' => $this->config['database']],
                'ResultConfiguration' => ['OutputLocation' => $this->config['s3output']],
            ]);

            $queryId = $result->get('QueryExecutionId');

            $status = 'QUEUED';
            while (in_array($status, ['QUEUED', 'RUNNING'])) {
                sleep(1);
                $exec = $this->athenaClient->getQueryExecution(['QueryExecutionId' => $queryId]);
                $status = $exec['QueryExecution']['Status']['State'];

                if (in_array($status, ['FAILED', 'CANCELLED'])) {
                    throw new Exception("Athena Query Failed: " . ($exec['QueryExecution']['Status']['StateChangeReason'] ?? 'unknown'));
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

    private function getDataWithQueryId(string $queryId): array
    {
        $response = $this->athenaClient->getQueryResults(['QueryExecutionId' => $queryId, 'MaxResults' => 500])->toArray();
        $rows = $response['ResultSet']['Rows'];

        while (!empty($response['NextToken'])) {
            $response = $this->athenaClient->getQueryResults([
                'QueryExecutionId' => $queryId,
                'NextToken' => $response['NextToken'],
                'MaxResults' => 500
            ])->toArray();

            $rows = array_merge($rows, $response['ResultSet']['Rows']);
        }

        return self::processResultRows($rows);
    }

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

    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        $hash = md5($this->prepareQuery($query, $bindings));

        // Try to fetch from cache, if table exists
        try {
            $cached = \Vasatiani\Athena\AthenaQueryHash::where('query_hash', $hash)->first();

            if ($cached) {
                return $this->getDataWithQueryId($cached->aws_return_id);
            }
        } catch (\Throwable $e) {
            // silently fail if table not found
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
        } catch (\Throwable $e) {
            // silently ignore
        }

        $results = $this->getDataWithQueryId($exec['QueryExecution']['QueryExecutionId']);
        $this->logQuery($query, [], $this->getElapsedTime($start));

        return $results;
    }

}
