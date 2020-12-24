<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Model\Message;
use AliyunMNS\Traits\MessagePropertiesForPeek;

class AliyunQueueMessage extends Message
{
    use MessagePropertiesForPeek;

    protected $body;

    public function __construct(
        $messageId, $messageBodyMD5, $messageBody, $enqueueTime, $nextVisibleTime, $firstDequeueTime, $dequeueCount,
        $priority, $receiptHandle
    )
    {

        $json = json_decode($messageBody, true);
        if (isset($json[AliyunQueue::SERIALIZATION_FLAG])
            && $json[AliyunQueue::SERIALIZATION_FLAG] == 'base64_serialize') {
            $this->body = unserialize(base64_decode($json['body']));
        }else{
            $this->body = $messageBody;
        }
       
        parent::__construct($messageId, $messageBodyMD5, $messageBody, $enqueueTime, $nextVisibleTime,
                            $firstDequeueTime, $dequeueCount, $priority, $receiptHandle);
    }

    /**
     * @return mixed
     */
    public function getBody()
    {

        return $this->body;
    }

    /**
     * @param mixed $body
     */
    public function setBody($body)
    {

        $this->body = $body;
    }
}
