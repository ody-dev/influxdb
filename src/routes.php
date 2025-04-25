<?php

/** @var \Ody\Foundation\Router\Router $router */

$router->get('/api/logs/recent', 'Ody\InfluxDB\Controllers\LogViewerController@recent');
$router->get('/api/logs/services', 'Ody\InfluxDB\Controllers\LogViewerController@services');
$router->get('/api/logs/levels', 'Ody\InfluxDB\Controllers\LogViewerController@levels');