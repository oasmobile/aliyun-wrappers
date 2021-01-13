<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Client;
use AliyunMNS\Model\MailAttributes;
use AliyunMNS\Model\MessageAttributes;
use AliyunMNS\Requests\PublishMessageRequest;
use AliyunMNS\Topic;
use Oasis\Mlib\AwsWrappers\Contracts\PublisherInterface;

class AliyunMNS implements PublisherInterface
{

    const CHANNEL_EMAIL = "email";
    /**
     * @var Client
     */
    protected $client;
    /** @var Topic */
    protected $topic;
    protected $accountName;

    public function __construct($accessId, $accessKey, $endPoint, $accountName, $topic)
    {

        $this->client      = new Client($endPoint, $accessId, $accessKey);
        $this->topic       = $this->client->getTopicRef($topic);
        $this->accountName = $accountName;
    }

    public function publish($subject, $body, $channels = [])
    {

        if (!is_array($channels)) {
            $channels = [$channels];
        }
        $messageAttributes = null;
        foreach ($channels as $channel) {
            switch ($channel) {
                case self::CHANNEL_EMAIL:
                    $mailAttributes    = new MailAttributes($subject, $this->accountName);
                    $messageAttributes = new MessageAttributes($mailAttributes);
                    break;
            }
        }
        $message = new PublishMessageRequest($body, null, $messageAttributes);

        return $this->topic->publishMessage($message);
    }
}
