<?php

namespace Vasatiani\Athena;

use Illuminate\Support\ServiceProvider;
use Illuminate\Database\ConnectionInterface;

class AthenaServiceProvider extends ServiceProvider
{
    /**
     * Indicates if loading of the provider is deferred.
     *
     * @var bool
     */
    protected $defer = false;

    /**
     * Check if we're running in Lumen.
     *
     * @return bool
     */
    protected function isLumen(): bool
    {
        return str_contains($this->app->version(), 'Lumen');
    }

    /**
     * Bootstrap any package services.
     */
    public function boot(): void
    {
        // Publish configuration (Laravel only, Lumen doesn't support this)
        if (!$this->isLumen() && method_exists($this, 'publishes')) {
            $this->publishes([
                __DIR__.'/../config/athena.php' => $this->getConfigPath('athena.php'),
            ], 'config');
        }
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

    /**
     * Get the config path for the given filename.
     *
     * @param string $filename
     * @return string
     */
    protected function getConfigPath(string $filename = ''): string
    {
        if ($this->isLumen()) {
            return base_path('config') . ($filename ? '/' . $filename : '');
        }

        return function_exists('config_path') 
            ? config_path($filename) 
            : base_path('config') . ($filename ? '/' . $filename : '');
    }
}
