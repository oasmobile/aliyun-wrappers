<?php

namespace Oasis\Mlib\AliyunLogging;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Logger;
use Oasis\Mlib\AliyunWrappers\AliyunMNS;

class AliyunLoggingHandler extends AbstractProcessingHandler
{
    /** @var AliyunMNS  */
    protected $publisher;
    protected $subject;

    private $isBatchHandling = false;
    private $contentBuffer   = '';

    public function __construct(AliyunMNS $publisher, $subject,$level = Logger::DEBUG, $bubble = true)
    {

        $this->publisher = $publisher;
        $this->subject = $subject;
        parent::__construct($level, $bubble);

    }

    public function handleBatch(array $records)
    {

        $this->isBatchHandling = true;
        parent::handleBatch($records);
        $this->isBatchHandling = false;
        $this->publishContent();
    }

    protected function publishContent()
    {

        if ($this->contentBuffer) {
            $this->publisher->publish($this->subject, $this->contentBuffer);
            $this->contentBuffer = '';
        }
    }

    protected function write(array $record)
    {

        if (!$this->isBatchHandling) {
            $this->contentBuffer = $record['formatted'];
            $this->publishContent();
        }
        else {
            $this->contentBuffer = $record['formatted'] . $this->contentBuffer;
        }

    }
}
