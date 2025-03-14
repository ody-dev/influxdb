<?php

Route::get('/api/logs/recent', 'Ody\InfluxDB\Controllers\LogViewerController@recent');
Route::get('/api/logs/services', 'Ody\InfluxDB\Controllers\LogViewerController@services');
Route::get('/api/logs/levels', 'Ody\InfluxDB\Controllers\LogViewerController@levels');