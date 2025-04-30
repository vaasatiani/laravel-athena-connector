<?php

namespace Vasatiani\Athena;

use Vasatiani\Athena\Eloquent\Builder as AthenaBuilder;
use Vasatiani\Athena\Eloquent\Collection as AthenaCollection;
use Illuminate\Database\Eloquent\Model as BaseModel;
use Illuminate\Database\Eloquent\Builder as BaseBuilder;

abstract class Model extends BaseModel
{
    /**
     * Set default connection name.
     *
     * @var string
     */
    protected $connection = 'athena';

    /**
     * Use custom Eloquent Builder.
     */
    public function newEloquentBuilder($query): BaseBuilder
    {
        return new AthenaBuilder($query);
    }

    /**
     * Use custom Collection when returning results.
     */
    public function newCollection(array $models = []): AthenaCollection
    {
        return new AthenaCollection($models);
    }
}
