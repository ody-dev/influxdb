<?php
/*
 * This file is part of ODY framework.
 *
 * @link     https://ody.dev
 * @document https://ody.dev/docs
 * @license  https://github.com/ody-dev/ody-core/blob/master/LICENSE
 */

namespace Ody\InfluxDB\Controllers;

use InfluxDB2\Client;
use InfluxDB2\QueryApi;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Message\ResponseInterface;
use Ody\Foundation\Http\Response;
use Psr\Log\LoggerInterface;

/**
 * Log Viewer Controller for InfluxDB 2.x
 *
 * Provides endpoints for retrieving logs from InfluxDB 2.x
 */
class InfluxDBLogViewerController
{
    /**
     * @var Client InfluxDB client
     */
    private $client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string Organization name
     */
    private $org;

    /**
     * @var string Bucket name
     */
    private $bucket;

    /**
     * @var QueryApi The query API
     */
    private $queryApi;

    /**
     * LogViewerController constructor
     *
     * Dependencies are automatically injected by the container
     */
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->logger = $logger;

        // Get org and bucket from config
        $this->org = config('influxdb.org', 'organization');
        $this->bucket = config('influxdb.bucket', 'logs');

        // Initialize query API
        $this->queryApi = $this->client->createQueryApi();
    }

    /**
     * Get recent logs
     *
     * @param ServerRequestInterface $request
     * @param ResponseInterface $response
     * @param array $params
     * @return ResponseInterface
     */
    public function recent(ServerRequestInterface $request, ResponseInterface $response, array $params): ResponseInterface
    {
        try {
            // Get query parameters
            $queryParams = $request->getQueryParams();

            // Default time range is last 5 minutes
            $timeRange = $queryParams['timeRange'] ?? '5m';

            // Optional service filter
            $service = $queryParams['service'] ?? null;

            // Optional level filter
            $level = $queryParams['level'] ?? null;

            // Build the Flux query
            $flux = "from(bucket: \"{$this->bucket}\")
                |> range(start: -{$timeRange})
                |> filter(fn: (r) => r._measurement == \"logs\")";

            // Add service filter if specified
            if ($service) {
                $flux .= "\n|> filter(fn: (r) => r.service == \"{$service}\")";
            }

            // Add level filter if specified
            if ($level) {
                $flux .= "\n|> filter(fn: (r) => r.level == \"{$level}\")";
            }

            // Order by time descending and limit results
            $limit = $queryParams['limit'] ?? 100;
            $flux .= "\n|> sort(columns: [\"_time\"], desc: true)
                |> limit(n: {$limit})";

            // Execute the query
            $tables = $this->queryApi->query($flux);

            // Process results
            $logs = [];
            foreach ($tables as $table) {
                foreach ($table->records as $record) {
                    // Build a unique key based on timestamp and service
                    $time = $record->getTime();
                    $service = $record->values["service"] ?? 'unknown';
                    $key = $time . '_' . $service;

                    // Initialize the log entry if it doesn't exist yet
                    if (!isset($logs[$key])) {
                        $logs[$key] = [
                            'timestamp' => $time,
                            'level' => $record->values["level"] ?? 'unknown',
                            'service' => $service,
                            'message' => '',  // Initialize with empty message
                            'context' => []
                        ];
                    }

                    // Handle the current field
                    $fieldName = $record->getField();
                    $fieldValue = $record->getValue();

                    // Assign the field to the appropriate place in the log entry
                    if ($fieldName === 'message') {
                        $logs[$key]['message'] = $fieldValue;
                    } elseif (!in_array($fieldName, ['level', 'service', '_time', '_measurement', '_field', '_value'])) {
                        $logs[$key]['context'][$fieldName] = $fieldValue;
                    }
                }
            }

            return $this->jsonResponse($response, [
                'success' => true,
                'logs' => array_values($logs), // Convert associative array to indexed array
                'count' => count($logs),
                'query' => [
                    'timeRange' => $timeRange,
                    'service' => $service,
                    'level' => $level,
                    'limit' => $limit,
                ],
            ]);
        } catch (\Exception $e) {
            $this->logger->error('Error retrieving logs', ['error' => $e->getMessage()]);

            return $this->jsonResponse($response->withStatus(500), [
                'success' => false,
                'error' => 'Failed to retrieve logs: ' . $e->getMessage(),
            ]);
        }
    }

    /**
     * Get available log services
     *
     * @param ServerRequestInterface $request
     * @param ResponseInterface $response
     * @param array $params
     * @return ResponseInterface
     */
    public function services(ServerRequestInterface $request, ResponseInterface $response, array $params): ResponseInterface
    {
        try {
            // Query for distinct service values using Flux
            $flux = "import \"influxdata/influxdb/schema\"
                
                schema.tagValues(
                    bucket: \"{$this->bucket}\",
                    tag: \"service\",
                    predicate: (r) => r._measurement == \"logs\"
                )";

            // Execute the query
            $tables = $this->queryApi->query($flux);

            $services = [];
            foreach ($tables as $table) {
                foreach ($table->records as $record) {
                    $services[] = $record->getValue();
                }
            }

            return $this->jsonResponse($response, [
                'success' => true,
                'services' => $services,
            ]);
        } catch (\Exception $e) {
            $this->logger->error('Error retrieving services', ['error' => $e->getMessage()]);

            return $this->jsonResponse($response->withStatus(500), [
                'success' => false,
                'error' => 'Failed to retrieve services: ' . $e->getMessage(),
            ]);
        }
    }

    /**
     * Get available log levels
     *
     * @param ServerRequestInterface $request
     * @param ResponseInterface $response
     * @param array $params
     * @return ResponseInterface
     */
    public function levels(ServerRequestInterface $request, ResponseInterface $response, array $params): ResponseInterface
    {
        try {
            // Query for distinct level values using Flux
            $flux = "import \"influxdata/influxdb/schema\"
                
                schema.tagValues(
                    bucket: \"{$this->bucket}\",
                    tag: \"level\",
                    predicate: (r) => r._measurement == \"logs\"
                )";

            // Execute the query
            $tables = $this->queryApi->query($flux);

            $levels = [];
            foreach ($tables as $table) {
                foreach ($table->records as $record) {
                    $levels[] = $record->getValue();
                }
            }

            return $this->jsonResponse($response, [
                'success' => true,
                'levels' => $levels,
            ]);
        } catch (\Exception $e) {
            $this->logger->error('Error retrieving log levels', ['error' => $e->getMessage()]);

            return $this->jsonResponse($response->withStatus(500), [
                'success' => false,
                'error' => 'Failed to retrieve log levels: ' . $e->getMessage(),
            ]);
        }
    }

    /**
     * Helper method to create JSON responses
     *
     * @param ResponseInterface $response
     * @param mixed $data
     * @return ResponseInterface
     */
    private function jsonResponse(ResponseInterface $response, $data): ResponseInterface
    {
        // Always set JSON content type
        $response = $response->withHeader('Content-Type', 'application/json');

        // If using our custom Response class
        if ($response instanceof Response) {
            return $response->withJson($data);
        }

        // For other PSR-7 implementations
        $response->getBody()->write(json_encode($data));
        return $response;
    }
}