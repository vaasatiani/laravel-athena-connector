<?php

namespace Vasatiani\Athena\Query;

use Illuminate\Database\Query\Processors\Processor as BaseProcessor;

/**
 * Query processor for AWS Athena.
 *
 * Currently uses the base processor as Athena result processing
 * is handled directly in the Connection class.
 */
class Processor extends BaseProcessor
{
    // No custom processing needed - handled in Connection::processResultRows()
}
