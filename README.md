# Laravel Athena Query Builder

A modern, clean, and scalable integration of AWS Athena with Laravel's Query Builder and Eloquent ORM.  
Built with full support for **Lumen 9** and **Laravel 9–12**, Redis-based concurrency limits, Octane/Swoole compatibility, and Athena-specific query and schema grammar.

---

## 🚀 Features

- ✅ Eloquent & Query Builder support
- ✅ Safe concurrency control (Athena allows max 20 concurrent queries)
- ✅ S3 query result management
- ✅ Redis lock support (Octane/Swoole-friendly)
- ✅ Schema-aware DDL generation (CREATE TABLE)
- ✅ Custom grammar for `BETWEENLIMIT`, Athena-safe `LIMIT/OFFSET`
- ✅ Fully configurable via `config/athena.php`

---

## 📦 Installation

```bash
composer require vasatiani/laravel-athena
```

### Laravel

Publish the config file:

```bash
php artisan vendor:publish --tag=config
```

### Lumen 9

Register the service provider in `bootstrap/app.php`:

```php
$app->register(Vasatiani\Athena\AthenaServiceProvider::class);
```

Copy the config file manually:

```bash
cp vendor/vasatiani/laravel-athena/config/athena.php config/athena.php
```

Load the config in `bootstrap/app.php`:

```php
$app->configure('athena');
```

Add the database connection in `config/database.php`:

```php
'connections' => [
    // ... other connections
    
    'athena' => [
        'driver' => 'athena',
    ],
],
```

---

## 🛠 Configuration

> Located in `config/athena.php`

```php
return [
    'credentials' => [
        'key'    => env('AWS_ATHENA_KEY', ''),
        'secret' => env('AWS_ATHENA_SECRET', ''),
    ],
    'region'        => env('AWS_ATHENA_REGION', 'eu-central-1'),
    'version'       => 'latest',
    'database'      => env('ATHENA_DB', ''),
    'prefix'        => env('ATHENA_TABLE_PREFIX', ''),
    'bucket'        => env('S3_ATHENA_BUCKET', ''),
    'outputfolder'  => env('ATHENA_OUTPUT_FOLDER', 'athena-output'),

    's3output'      => 's3://' . env('S3_BUCKET', '') . '/' . env('ATHENA_OUTPUT_FOLDER', 'athena-output'),

    // Concurrency control
    'lock_key'      => env('ATHENA_LOCK_KEY', 'athena:query:concurrency'),
    'lock_timeout'  => env('ATHENA_LOCK_TIMEOUT', 10),
    'lock_wait'     => env('ATHENA_LOCK_WAIT', 5),

    // Schema creation options
    'stored_as' => 'PARQUET',
    'table_properties' => [
        'has_encrypted_data' => 'false',
        'classification'     => 'json',
    ],
];
```

Make sure your `.env` contains:

```env
AWS_ATHENA_KEY=your-key
AWS_ATHENA_SECRET=your-secret
AWS_ATHENA_REGION=eu-central-1
S3_ATHENA_BUCKET=your-bucket
ATHENA_DB=your_database
ATHENA_OUTPUT_FOLDER=athena-output
```

---

## 🧩 Usage

### 🏗 Defining a Model

```php
namespace App\Models;

use Vasatiani\Athena\Model;

class AthenaEvent extends Model
{
    protected $table = 'events';
}
```

---

### 📋 Running Queries

```php
use App\Models\AthenaEvent;

$events = AthenaEvent::where('event_type', 'login')->get();

$events = AthenaEvent::query()
    ->whereDateBetween('created_at', '2024-01-01', '2024-01-31')
    ->get();

$events = AthenaEvent::whereRegex('event_name', '^checkout_.*')->get();
```

---

### 🔐 Automatic Query Caching

Queries are hashed and stored in the `athena_query_hashes` table.

> If the table is missing, the system will fallback gracefully.

Migration:

```php
Schema::create('athena_query_hashes', function (Blueprint $table) {
    $table->string('query_hash')->primary();
    $table->string('aws_return_id');
    $table->timestamps();
});
```

---

### 💥 Custom Query Builder Helpers

```php
AthenaEvent::whereNotEmpty('email')->get();
AthenaEvent::selectIfEmpty(['id', 'name'])->get();
AthenaEvent::whereJsonHasKey('payload', 'device_id')->get();
```

---

## 🧱 Schema Support (Optional)

```php
Schema::connection('athena')->create('events', function (Blueprint $table) {
    $table->string('event_type');
    $table->timestamp('created_at');
});
```

This generates:

```sql
CREATE EXTERNAL TABLE events (
  event_type string,
  created_at timestamp
)
STORED AS PARQUET
LOCATION 's3://your-bucket/athena-output'
TBLPROPERTIES ('has_encrypted_data'='false','classification'='json')
```

---

## ⚠️ Athena LIMIT & OFFSET Handling

Internally converted to `ROW_NUMBER()` queries via special syntax `BETWEENLIMIT`.

---

## ✅ Redis Lock Integration

Uses Laravel's `Cache::lock()` to prevent exceeding Athena’s concurrency limits.

---

## 🧪 Testing

```php
DB::connection('athena')->select("SELECT count(*) as total FROM users");
```

---

## 📘 License

MIT © [Vakho Asatiani](mailto:vaasatiani@gmail.com)
