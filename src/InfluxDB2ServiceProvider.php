<?php
/*
 * This file is part of InfluxDB2 Logger for ODY framework.
 *
 * @link     https://github.com/example/influxdb2-logger
 * @license  MIT
 */

namespace Ody\InfluxDB;

use InfluxDB2\Client;
use Ody\Logger\LogManager;
use Ody\Foundation\Providers\ServiceProvider;
use Ody\InfluxDB\Logging\InfluxDB2Logger;

/**
 * InfluxDB2 Service Provider
 *
 * Registers InfluxDB 2.x logger in the ODY framework.
 */
class InfluxDB2ServiceProvider extends ServiceProvider
{
    /**
     * Register the InfluxDB 2.x logger
     *
     * @return void
     */
    public function register(): void
    {
        // Register the InfluxDB\Logging namespace for auto-discovery
        $this->container->make(LogManager::class)->registerDriver('influxdb', InfluxDB2Logger::class);
    }

    /**
     * Bootstrap the service provider
     *
     * @return void
     */
    public function boot(): void
    {
        // Register InfluxDB client as a singleton for dependency injection
        $this->singleton(Client::class, function () {
            $config = $this->container->make('config');

            $options = [
                "url" => $config->get('influxdb.url', 'http://localhost:8086'),
                "token" => $config->get('influxdb.token', ''),
                "bucket" => $config->get('influxdb.bucket', 'logs'),
                "org" => $config->get('influxdb.org', 'organization'),
                "precision" => \InfluxDB2\Model\WritePrecision::S
            ];

            return new Client($options);
        });
    }
}