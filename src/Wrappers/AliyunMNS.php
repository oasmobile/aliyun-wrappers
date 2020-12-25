<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Client;
use AliyunMNS\Requests\PublishMessageRequest;
use AliyunMNS\Topic;
use Oasis\Mlib\AwsWrappers\Contracts\PublisherInterface;

class AliyunMNS implements PublisherInterface
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

    public function publish($subject, $body, $channels = [])
    {
        $message = new PublishMessageRequest($body);
        $this->topic->publishMessage($message);
    }
}
