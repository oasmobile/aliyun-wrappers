<?php

namespace Oasis\Mlib\AliyunWrappers;

use AliyunMNS\Client;
use AliyunMNS\Model\Message;
use AliyunMNS\Model\QueueAttributes;
use AliyunMNS\Model\SendMessageRequestItem;
use AliyunMNS\Queue;
use AliyunMNS\Requests\BatchReceiveMessageRequest;
use AliyunMNS\Requests\BatchSendMessageRequest;
use Exception;
use InvalidArgumentException;
use Oasis\Mlib\AwsWrappers\Contracts\QueueInterface;

class AliyunQueue implements QueueInterface
{

    public const SERIALIZATION_FLAG = '_serialization';
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Queue
     */
    protected $queue;

    public function __construct($accessId, $accessKey, $endPoint, $name)
    {
        $this->client = new Client($endPoint, $accessId, $accessKey);
        $this->queue  = $this->client->getQueueRef($name);
    }

    public function sendMessage($payroll, $delay = 0, $attributes = [])
    {
        $sentMessages = $this->sendMessages([$payroll], $delay, $attributes);
        if (!$sentMessages) {
            return false;
        }
        else {
            return $sentMessages[0];
        }
    }

    public function sendMessages(array $payrolls, $delay = 0, array $attributesList = [], $concurrency = 10)
    {
        $record = [];
        foreach ($payrolls as $payroll) {
            if (!is_string($payroll)) {
                $payroll = json_encode(
                    [
                        self::SERIALIZATION_FLAG => 'base64_serialize',
                        "body"                   => base64_encode(serialize($payroll)),
                    ]
                );
            }
            $record[] = new SendMessageRequestItem($payroll, $delay);
        }

        $response = $this->queue->batchSendMessage(new BatchSendMessageRequest($record));

        return $response->getSendMessageResponseItems();
    }

    public function receiveMessage($wait = null, $visibility_timeout = null, $metas = [], $message_attributes = [])
    {
        $ret = $this->receiveMessageBatch(1, $wait);
        if (!$ret) {
            return null;
        }
        else {
            return $ret[0];
        }
    }

    /**
     * @param      $max_count
     * @param  null  $wait
     *
     * @return Message[]
     */
    public function receiveMessages($max_count, $wait = null)
    {
        if ($max_count <= 0) {
            return [];
        }

        $buffer    = [];
        $one_batch = 10;
        while ($msgs = $this->receiveMessageBatch(
            $one_batch,
            $wait
        )) {
            $buffer    = array_merge($buffer, $msgs);
            $one_batch = min(10, $max_count - count($buffer));
            if ($one_batch <= 0) {
                break;
            }
        }

        return $buffer;
    }


    public function deleteMessage($msg)
    {
        $this->deleteMessages([$msg]);
    }

    public function deleteMessages($messages)
    {
        $receiptHandles = [];
        /** @var Message[] $messages */
        foreach ($messages as $message) {
            $receiptHandles[] = $message->getReceiptHandle();
        }

        try {
            $this->queue->batchDeleteMessage($receiptHandles);
        } catch (Exception $e) {
        }
    }

    public function getAttribute($name)
    {
        return $this->getAttributes([$name]);
    }

    public function getAttributes(array $attributeNames)
    {
        /** @var QueueAttributes $queueAttributes */
        $queueAttributes = $this->queue->getAttribute()->getQueueAttributes();
        $attributes      = [
            'DelaySeconds'                          => $queueAttributes->getDelaySeconds(),
            'MaximumMessageSize'                    => $queueAttributes->getMaximumMessageSize(),
            'MessageRetentionPeriod'                => $queueAttributes->getMessageRetentionPeriod(),
            'ReceiveMessageWaitTimeSeconds'         => $queueAttributes->getPollingWaitSeconds(),
            'VisibilityTimeout'                     => $queueAttributes->getVisibilityTimeout(),
            'All'                                   => '',
            'ApproximateNumberOfMessages'           => $queueAttributes->getActiveMessages(),
            'ApproximateNumberOfMessagesNotVisible' => $queueAttributes->getInactiveMessages(),
            'ApproximateNumberOfMessagesDelayed'    => $queueAttributes->getDelayMessages(),
            'CreatedTimestamp'                      => $queueAttributes->getCreateTime(),
            'LastModifiedTimestamp'                 => $queueAttributes->getLastModifyTime(),
            'QueueArn'                              => $queueAttributes->getQueueName(),
        ];

        $array = [];
        foreach ($attributeNames as $name) {
            if (isset($attributes[$name])) {
                $array[$name] = $attributes[$name];
            }
        }

        return $array;
    }

    protected function receiveMessageBatch(
        $maxCount = 1,
        $wait = null
    ) {
        if ($maxCount > 10 || $maxCount < 1) {
            throw new InvalidArgumentException("Max count for queue message receiving is 10");
        }
        $messages = [];
        try {
            $request = new BatchReceiveMessageRequest($maxCount, $wait);
            $reponse = $this->queue->batchReceiveMessage($request);

            foreach ($reponse->getMessages() as $message) {
                $messages[] = new AliyunQueueMessage(
                    $message->getMessageId(),
                    $message->getMessageBodyMD5(),
                    $message->getMessageBody(),
                    $message->getEnqueueTime(),
                    $message->getNextVisibleTime(),
                    $message->getFirstDequeueTime(),
                    $message->getDequeueCount(),
                    $message->getPriority(),
                    $message->getReceiptHandle()
                );
            }
        } catch (Exception $e) {
        }

        return $messages;
    }
}
