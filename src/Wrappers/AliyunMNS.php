<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Client;
use AliyunMNS\Requests\PublishMessageRequest;

class AliyunMNS
{

    /**
     * @var \AliyunMNS\Client
     */
    protected $client;
    /** @var \AliyunMNS\Topic */
    protected $topic;

    public function __construct($accessId, $accessKey, $endPoint, $topic)
    {

        $this->client = new Client($endPoint, $accessId, $accessKey);
        $this->topic  = $this->client->getTopicRef($topic);
    }


    public function publish($subject, $body)
    {

        $message = new PublishMessageRequest($body);
        $this->topic->publishMessage($message);
    }
}
