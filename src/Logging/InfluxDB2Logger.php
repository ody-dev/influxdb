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
use Ody\Foundation\Logging\AbstractLogger;
use Ody\Foundation\Logging\FormatterInterface;
use Ody\Foundation\Logging\JsonFormatter;
use Ody\Foundation\Logging\LineFormatter;
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
        $this->useCoroutines = $useCoroutines && extension_loaded('swoole');

        // Get write API with batching options
        $this->writeApi = $this->client->createWriteApi([
            'writeType' => WriteType::BATCHING,
            'batchSize' => 1000,
            'flushInterval' => 1000
        ]);
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
            "precision" => $config['precision'] ?? WritePrecision::S
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
        $this->useCoroutines = $enable && extension_loaded('swoole');
        return $this;
    }

    /**
     * Destructor: ensure data is flushed
     */
    public function __destruct()
    {
        // Flush any remaining points in the buffer
        try {
            $this->writeApi->close();
        } catch (Throwable $e) {
            error_log('Error closing InfluxDB write API: ' . $e->getMessage());
        }
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

        // Add message as a field
        $point->addField('message', $message);

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

        // Write the point - use coroutines if enabled
        if ($this->useCoroutines && Coroutine::getCid() >= 0) {
            // Use Swoole coroutine for non-blocking writes
            Coroutine::create(function () use ($point) {
                try {
                    $this->writeApi->write($point);
                } catch (Throwable $e) {
                    error_log('Error writing to InfluxDB: ' . $e->getMessage());
                }
            });
        } else {
            // Synchronous write
            try {
                $this->writeApi->write($point);
            } catch (Throwable $e) {
                error_log('Error writing to InfluxDB: ' . $e->getMessage());
            }
        }
    }
}