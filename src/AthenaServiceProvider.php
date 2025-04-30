<?php

namespace Vasatiani\Athena;

use Illuminate\Support\ServiceProvider;
use Illuminate\Database\ConnectionInterface;

class AthenaServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap any package services.
     */
    public function boot(): void
    {
        // Publish configuration
        $this->publishes([
            __DIR__.'/../config/athena.php' => config_path('athena.php'),
        ], 'config');
    }

    /**
     * Register the package services.
     */
    public function register(): void
    {
        // Merge default config
        $this->mergeConfigFrom(
            __DIR__.'/../config/athena.php', 'athena'
        );

        // Register custom database connection
        $this->app['db']->extend('athena', function ($config, $name): ConnectionInterface {
            return new Connection($config);
        });
    }
}
