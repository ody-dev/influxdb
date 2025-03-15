<?php
/*
 * This file is part of InfluxDB2 Logger for ODY framework.
 *
 * @link     https://github.com/example/influxdb2-logger
 * @license  MIT
 */

namespace Ody\InfluxDB\Logging;

use InfluxDB2\Client;
use InfluxDB2\Model\WritePrecision;
use InfluxDB2\Point;
use InfluxDB2\WriteApi;
use InfluxDB2\WriteType;
use Ody\Logger\AbstractLogger;
use Ody\Logger\FormatterInterface;
use Ody\Logger\JsonFormatter;
use Ody\Logger\LineFormatter;
use Ody\Swoole\Coroutine\ContextManager;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Swoole\Coroutine;
use Throwable;

/**
 * InfluxDB 2.x Logger
 * Logs messages to InfluxDB 2.x time-series database
 */
class InfluxDB2Logger extends AbstractLogger
{
    /**
     * @var Client InfluxDB client
     */
    protected Client $client;

    /**
     * @var WriteApi InfluxDB write API
     */
    protected WriteApi $writeApi;

    /**
     * @var string Bucket to write to
     */
    protected string $bucket;

    /**
     * @var string Organization
     */
    protected string $org;

    /**
     * @var string Measurement name for logs
     */
    protected string $measurement = 'logs';

    /**
     * @var array Default tags to include with every log entry
     */
    protected array $defaultTags = [];

    /**
     * @var bool Whether to use Swoole coroutines for non-blocking writes
     */
    protected bool $useCoroutines = false;

    /**
     * @var bool Whether we're running in a Swoole environment
     */
    protected bool $isSwooleEnv = false;

    /**
     * @var int Flush interval in milliseconds
     */
    protected int $flushInterval = 1000;

    /**
     * @var int Batch size
     */
    protected int $batchSize = 1000;

    /**
     * @var array Pending points to write
     */
    protected array $pendingPoints = [];

    /**
     * @var int Last flush time
     */
    protected int $lastFlushTime = 0;

    /**
     * Constructor
     *
     * @param Client $client InfluxDB client instance
     * @param string $org InfluxDB organization
     * @param string $bucket InfluxDB bucket
     * @param string $measurement Measurement name for logs
     * @param array $defaultTags Default tags for all log entries
     * @param bool $useCoroutines Whether to use Swoole coroutines
     * @param string $level Minimum log level
     * @param FormatterInterface|null $formatter
     */
    public function __construct(
        Client $client,
        string $org,
        string $bucket,
        string $measurement = 'logs',
        array $defaultTags = [],
        bool $useCoroutines = false,
        string $level = LogLevel::DEBUG,
        ?FormatterInterface $formatter = null
    ) {
        parent::__construct($level, $formatter);

        $this->client = $client;
        $this->org = $org;
        $this->bucket = $bucket;
        $this->measurement = $measurement;
        $this->defaultTags = $defaultTags;
        $this->isSwooleEnv = $useCoroutines;
        $this->useCoroutines = $useCoroutines;
        $this->lastFlushTime = time() * 1000; // Current time in milliseconds

        // Get write API with batching options - use different settings for Swoole
        if ($this->isSwooleEnv) {
            // For Swoole, we'll manage our own batching to avoid issues with coroutines
            $this->writeApi = $this->client->createWriteApi([
                'writeType' => WriteType::SYNCHRONOUS, // Use synchronous writes in Swoole
                'debug' => env('APP_DEBUG', false)
            ]);
        } else {
            // Standard batching for non-Swoole environments
            $this->writeApi = $this->client->createWriteApi([
                'writeType' => WriteType::BATCHING,
                'batchSize' => $this->batchSize,
                'flushInterval' => $this->flushInterval,
                'debug' => env('APP_DEBUG', false)
            ]);
        }

        // Register shutdown function to ensure logs are flushed
        register_shutdown_function([$this, 'flush']);
    }

    /**
     * Create an InfluxDB2 logger from configuration
     *
     * @param array $config
     * @return LoggerInterface
     * @throws \InvalidArgumentException
     */
    public static function create(array $config): LoggerInterface
    {
        // Validate required configuration
        if (!isset($config['url'])) {
            throw new \InvalidArgumentException("InfluxDB2 logger requires a 'url' configuration value");
        }

        if (!isset($config['token'])) {
            throw new \InvalidArgumentException("InfluxDB2 logger requires a 'token' configuration value");
        }

        if (!isset($config['org'])) {
            throw new \InvalidArgumentException("InfluxDB2 logger requires an 'org' configuration value");
        }

        if (!isset($config['bucket'])) {
            throw new \InvalidArgumentException("InfluxDB2 logger requires a 'bucket' configuration value");
        }

        // Create InfluxDB client
        $client = new Client([
            "url" => $config['url'],
            "token" => $config['token'],
            "bucket" => $config['bucket'],
            "org" => $config['org'],
            "precision" => $config['precision'] ?? WritePrecision::S,
            "debug" => $config['debug'] ?? false,
        ]);

        // Default tags
        $defaultTags = [
            'service' => $config['service'] ?? env('APP_NAME', 'ody-service'),
            'environment' => $config['environment'] ?? env('APP_ENV', 'production'),
            'host' => $config['host'] ?? gethostname(),
        ];

        // Merge with custom tags if provided
        if (isset($config['tags']) && is_array($config['tags'])) {
            $defaultTags = array_merge($defaultTags, $config['tags']);
        }

        // Create formatter if specified
        $formatter = null;
        if (isset($config['formatter'])) {
            $formatter = self::createFormatter($config);
        }

        // Create and return the logger
        return new self(
            $client,
            $config['org'],
            $config['bucket'],
            $config['measurement'] ?? 'logs',
            $defaultTags,
            $config['use_coroutines'] ?? false,
            $config['level'] ?? LogLevel::DEBUG,
            $formatter
        );
    }

    /**
     * Create a formatter based on configuration
     *
     * @param array $config
     * @return FormatterInterface
     */
    protected static function createFormatter(array $config): FormatterInterface
    {
        $formatterType = $config['formatter'] ?? 'json';

        switch ($formatterType) {
            case 'line':
                return new LineFormatter(
                    $config['format'] ?? null,
                    $config['date_format'] ?? null
                );
            case 'json':
            default:
                return new JsonFormatter();
        }
    }

    /**
     * Set the measurement name
     *
     * @param string $measurement
     * @return self
     */
    public function setMeasurement(string $measurement): self
    {
        $this->measurement = $measurement;
        return $this;
    }

    /**
     * Add default tags
     *
     * @param array $tags
     * @return self
     */
    public function addDefaultTags(array $tags): self
    {
        $this->defaultTags = array_merge($this->defaultTags, $tags);
        return $this;
    }

    /**
     * Enable or disable coroutines
     *
     * @param bool $enable
     * @return self
     */
    public function useCoroutines(bool $enable): self
    {
        $this->useCoroutines = $enable && $this->isSwooleEnv;
        return $this;
    }

    /**
     * Flush all pending points to InfluxDB
     */
    public function flush(): void
    {
        try {
            // If we're in Swoole and have pending points, flush them directly
            if ($this->isSwooleEnv && !empty($this->pendingPoints)) {
                $pointCount = count($this->pendingPoints);
                if ($pointCount > 0) {
                    error_log("InfluxDB2Logger: Flushing {$pointCount} pending points");
                    $this->writeApi->write($this->pendingPoints);
                    $this->pendingPoints = [];
                    $this->lastFlushTime = time() * 1000;
                }
            }

            // Close the write API to flush any remaining points in the buffer
            $this->writeApi->close();
        } catch (Throwable $e) {
            error_log('Error flushing InfluxDB data: ' . $e->getMessage());
        }
    }

    /**
     * Destructor: ensure data is flushed
     */
    public function __destruct()
    {
        $this->flush();
    }

    /**
     * Get IP address from Swoole context or server params
     *
     * @return string
     */
    protected function getClientIp(): string
    {
        // Try to get IP from Swoole coroutine context
        if ($this->isSwooleEnv && class_exists('\Ody\Swoole\Coroutine\ContextManager')) {
            $serverParams = ContextManager::get('_SERVER');

            if (is_array($serverParams)) {
                // Try common IP address headers and variables
                if (!empty($serverParams['remote_addr'])) {
                    return $serverParams['remote_addr'];
                }

                if (!empty($serverParams['HTTP_X_FORWARDED_FOR'])) {
                    // X-Forwarded-For may contain multiple IPs, take the first one
                    $ips = explode(',', $serverParams['HTTP_X_FORWARDED_FOR']);
                    return trim($ips[0]);
                }

                if (!empty($serverParams['HTTP_CLIENT_IP'])) {
                    return $serverParams['HTTP_CLIENT_IP'];
                }
            }
        }

        // Fallback to regular server params if not in Swoole
        if (!$this->isSwooleEnv) {
            if (!empty($_SERVER['REMOTE_ADDR'])) {
                return $_SERVER['REMOTE_ADDR'];
            }

            if (!empty($_SERVER['HTTP_X_FORWARDED_FOR'])) {
                $ips = explode(',', $_SERVER['HTTP_X_FORWARDED_FOR']);
                return trim($ips[0]);
            }

            if (!empty($_SERVER['HTTP_CLIENT_IP'])) {
                return $_SERVER['HTTP_CLIENT_IP'];
            }
        }

        return 'unknown';
    }

    /**
     * {@inheritdoc}
     */
    protected function write(string $level, string $message, array $context = []): void
    {
        // Create a data point for InfluxDB
        $point = Point::measurement($this->measurement)
            ->addTag('level', strtolower($level));

        // Add default tags
        foreach ($this->defaultTags as $key => $value) {
            $point->addTag($key, (string)$value);
        }

        // Format the message with context if it's empty or generic
        $formattedMessage = $message;

        // Check if message is empty or a generic one from the framework
        $genericMessages = [
            'Request started',
            'Request completed',
            'Request failed'
        ];

        if (empty(trim($message)) || in_array($message, $genericMessages)) {
            // For request logs, create a meaningful message
            if (isset($context['method']) && isset($context['uri'])) {
                $formattedMessage = "{$context['method']} {$context['uri']}";

                // Add status if available
                if (isset($context['status'])) {
                    $formattedMessage .= " - {$context['status']}";

                    // Add duration if available
                    if (isset($context['duration'])) {
                        $formattedMessage .= " ({$context['duration']})";
                    }
                }
            }
        }

        // Always provide a non-empty message
        if (empty(trim($formattedMessage))) {
            $formattedMessage = "Log entry at " . date('Y-m-d H:i:s');
        }

        // Add the formatted message as a field
        $point->addField('message', $formattedMessage);

        // Extract error information if available
        if (isset($context['error']) && $context['error'] instanceof Throwable) {
            $error = $context['error'];
            $point->addField('error_message', $error->getMessage());
            $point->addField('error_file', $error->getFile());
            $point->addField('error_line', (string)$error->getLine());
            $point->addField('error_trace', $error->getTraceAsString());
        }

        // Add custom tags from context
        if (isset($context['tags']) && is_array($context['tags'])) {
            foreach ($context['tags'] as $key => $value) {
                $point->addTag($key, (string)$value);
            }
        }

        // Handle IP address - if it's unknown, try to get it from Swoole context
        if (isset($context['ip']) && $context['ip'] === 'unknown') {
            $context['ip'] = $this->getClientIp();
        }

        // Add other context fields, excluding 'tags' and 'error' which are handled separately
        foreach ($context as $key => $value) {
            if ($key !== 'tags' && $key !== 'error') {
                // Convert arrays and objects to JSON strings
                if (is_array($value) || is_object($value)) {
                    $value = json_encode($value);
                }

                // Only add scalar values as fields
                if (is_scalar($value) || is_null($value)) {
                    $point->addField($key, $value);
                }
            }
        }

        error_log('------------ Influx write ---------------');

        // Handle differently depending on environment
        if ($this->isSwooleEnv) {
            // In Swoole, add to our pending points and flush if needed
            $this->pendingPoints[] = $point;

            // Check if we should flush based on batch size or time
            $currentTime = time() * 1000;
            $timeElapsed = $currentTime - $this->lastFlushTime;

            if (count($this->pendingPoints) >= $this->batchSize || $timeElapsed >= $this->flushInterval) {
                // Flush immediately in the current context
                try {
                    error_log("Immediate flush of " . count($this->pendingPoints) . " points");
                    $this->writeApi->write($this->pendingPoints);
                    $this->pendingPoints = [];
                    $this->lastFlushTime = $currentTime;
                } catch (Throwable $e) {
                    error_log('Error writing to InfluxDB: ' . $e->getMessage());
                }
            }

            // Schedule a delayed flush for any remaining points
            // This ensures that logs get written even if batch size isn't reached
            if ($this->useCoroutines && Coroutine::getCid() >= 0 && !empty($this->pendingPoints)) {
                // Use coroutine to flush remaining points after the request completes
                Coroutine::create(function () {
                    // Small delay to allow request to complete
                    usleep(100000); // 100ms
                    if (!empty($this->pendingPoints)) {
                        error_log("Delayed flush of " . count($this->pendingPoints) . " points from coroutine");
                        $this->flush();
                    }
                });
            }
        } else {
            // In regular PHP, use the batching API as normal
            try {
                $this->writeApi->write($point);
            } catch (Throwable $e) {
                error_log('Error writing to InfluxDB: ' . $e->getMessage());
            }
        }
    }
}