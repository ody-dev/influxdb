# InfluxDB2 Logger for ODY Framework

A custom logging solution for ODY Framework that stores logs in InfluxDB 2.x, a time-series database designed for high-write and query loads.

## Features

- Store logs in InfluxDB 2.x with efficient time series format
- Support for tagging logs for powerful querying and filtering
- Asynchronous logging with Swoole coroutines for high performance
- Configurable measurement names, tags, and fields
- Seamless integration with ODY Framework logging system

## Installation

```bash
composer require ody/influxdb
```

## Configuration

### Register the Service Provider

Add the InfluxDB2 service provider to your `config/app.php` configuration:

```php
'providers' => [
    // Other providers
    InfluxDB\Logging\InfluxDB2ServiceProvider::class,
],
```

### Configure the Logger

Add an InfluxDB channel to your `config/logging.php` configuration:

```php
'channels' => [
    // Other channels...
    
    'influxdb' => [
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
    
    // Optional: Add to stack to log to multiple destinations
    'production' => [
        'driver' => 'group',
        'channels' => ['file', 'influxdb'],
    ],
],
```

### Environment Variables

Configure the following environment variables in your `.env` file:

```
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_token_here
INFLUXDB_ORG=your_org
INFLUXDB_BUCKET=logs
INFLUXDB_LOG_LEVEL=debug
INFLUXDB_USE_COROUTINES=false
```

## Usage

### Basic Usage

```php
// Log to InfluxDB channel
logger('User registered', ['user_id' => 123], 'influxdb');

// Or configure influxdb as your default channel in config/logging.php
// and use logger normally
logger('System started');
```

### Using Tags for Better Querying

Tags in InfluxDB are indexed, making filtering by tags very efficient:

```php
// Log with custom tags
logger('API request processed', [
    'duration' => 120,
    'endpoint' => '/users',
    'tags' => [
        'module' => 'api',
        'method' => 'GET',
        'status' => 200
    ]
], 'influxdb');
```

### Dependency Injection

You can also inject the logger directly:

```php
use Psr\Log\LoggerInterface;

class UserService
{
    protected $logger;
    
    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
    
    public function registerUser($data)
    {
        // Register user logic...
        
        $this->logger->info('User registered', [
            'id' => $user->id,
            'email' => $user->email
        ]);
    }
}
```

## Advanced Configuration

### Using with Swoole Coroutines

For high-performance applications using Swoole, enable coroutines to make logging non-blocking:

```php
'influxdb' => [
    // ...other config
    'use_coroutines' => true,
],
```

This will use Swoole coroutines to perform writes to InfluxDB asynchronously, preventing your application from blocking during log writes.

### Custom Measurement Names

You can specify a custom measurement name:

```php
'influxdb' => [
    // ...other config
    'measurement' => 'application_logs',
],
```

### Grouping Related Logs

You can group related logs by adding the same tag values:

```php
$requestId = uniqid();

logger('Request started', [
    'tags' => ['request_id' => $requestId]
]);

// Later in the code
logger('Database query executed', [
    'query' => 'SELECT * FROM users',
    'duration' => 25,
    'tags' => ['request_id' => $requestId]
]);

// And finally
logger('Request completed', [
    'duration' => 150,
    'tags' => ['request_id' => $requestId]
]);
```

This allows you to query InfluxDB for all logs related to a specific request.

## Querying Logs

You can query your logs directly in InfluxDB using Flux or InfluxQL. Here are some examples:

### Get recent logs:

```flux
from(bucket: "logs")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "logs")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 100)
```

### Filter by level:

```flux
from(bucket: "logs")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "logs")
  |> filter(fn: (r) => r.level == "error")
```

### Filter by custom tag:

```flux
from(bucket: "logs")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "logs")
  |> filter(fn: (r) => r.method == "GET")
  |> filter(fn: (r) => r.status == "404")
```

## Multi-Environment Configuration

For multi-environment deployments, configure each environment separately:

```
# Development .env
INFLUXDB_URL=http://127.0.0.0.1:8086
INFLUXDB_BUCKET=logs_dev

# Production .env
INFLUXDB_URL=https://influxdb.example.com
INFLUXDB_BUCKET=logs_prod
```

## License

MIT