<?php
/*
 * This file is part of InfluxDB2 Logger for ODY framework.
 *
 * @link     https://github.com/example/influxdb2-logger
 * @license  MIT
 */

return [
    /*
    |--------------------------------------------------------------------------
    | InfluxDB 2.x Configuration
    |--------------------------------------------------------------------------
    |
    | This file defines the configuration settings for the InfluxDB 2.x integration.
    |
    */

    // Connection settings
    'url' => env('INFLUXDB_URL', 'http://127.0.0.1:8086'),
    'token' => env('INFLUXDB_TOKEN', ''),
    'org' => env('INFLUXDB_ORG', 'organization'),
    'bucket' => env('INFLUXDB_BUCKET', 'logs'),
    'precision' => env('INFLUXDB_PRECISION', 's'), // s, ms, us, ns

    // Logging settings
    'level' => env('INFLUXDB_LOG_LEVEL', 'debug'),
    'measurement' => env('INFLUXDB_MEASUREMENT', 'logs'),
    'use_coroutines' => env('INFLUXDB_USE_COROUTINES', false),

    // Optional: Default tags to include with all log entries
    'tags' => [
        'service' => env('APP_NAME', 'ody-service'),
        'environment' => env('APP_ENV', 'production'),
        'instance' => env('INSTANCE_ID', gethostname()),
    ],

    // Configure the logger in the logging.php configuration
    'log_channel' => [
        'driver' => 'influxdb',
        'url' => env('INFLUXDB_URL', 'http://127.0.0.1:8086'),
        'token' => env('INFLUXDB_TOKEN', ''),
        'org' => env('INFLUXDB_ORG', 'organization'),
        'bucket' => env('INFLUXDB_BUCKET', 'logs'),
        'measurement' => env('INFLUXDB_MEASUREMENT', 'logs'),
        'level' => env('INFLUXDB_LOG_LEVEL', 'debug'),
        'use_coroutines' => env('INFLUXDB_USE_COROUTINES', false),
        'tags' => [
            'service' => env('APP_NAME', 'ody-service'),
            'environment' => env('APP_ENV', 'production'),
            'instance' => env('INSTANCE_ID', gethostname()),
        ],
    ],
];