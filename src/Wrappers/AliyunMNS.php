<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Client;
use AliyunMNS\Requests\PublishMessageRequest;
use AliyunMNS\Topic;

class AliyunMNS
{
    /**
     * @var Client
     */
    protected $client;
    /** @var Topic */
    protected $topic;

    public function __construct($accessId, $accessKey, $endPoint, $topic)
    {
        $this->client = new Client($endPoint, $accessId, $accessKey);
        $this->topic  = $this->client->getTopicRef($topic);
    }

    /** @noinspection PhpUnusedParameterInspection */
    public function publish($subject, $body)
    {
        $message = new PublishMessageRequest($body);
        $this->topic->publishMessage($message);
    }
}
