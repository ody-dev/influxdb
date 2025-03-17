# InfluxDB2 Logger for ODY Framework

A custom logging solution for ODY Framework that stores logs in InfluxDB 2.x.

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

## License

MIT
