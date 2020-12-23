<?php

namespace Oasis\Mlib\AliyunWrappers\Test;

use Oasis\Mlib\AliyunWrappers\AliyunMNS;
use Oasis\Mlib\AliyunWrappers\AliyunQueue;
use Oasis\Mlib\Utils\ArrayDataProvider;
use Oasis\Mlib\Utils\DataProviderInterface;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class AliyunQueueTest extends TestCase
{

    static $topid;
    static $queue;

    public static function setUpBeforeClass()
    {

        parent::setUpBeforeClass();

        $dataSourceFile = __DIR__ . "/config.yml";
        $config         = Yaml::parse(file_get_contents($dataSourceFile));
        $dp             = new ArrayDataProvider($config);
        $config         = $dp->getMandatory('mns', DataProviderInterface::ARRAY_TYPE);

        self::$topid = new AliyunMNS(
            $config['access_key'],
            $config['access_secret'],
            $config['end_point'],
            $config['topic']
        );

        self::$queue = new AliyunQueue(
            $config['access_key'],
            $config['access_secret'],
            $config['end_point'],
            $config['queue']
        );

    }

    public function testQueue()
    {

        self::$queue->sendMessage('xxxx');
        $message = self::$queue->receiveMessage();
        print_r($message);
        $this->assertEquals("xxxx", $message->getBody());

        self::$queue->deleteMessage($message);
    }

    public function atestBatchQueue()
    {
        $array = ['x','xx','xxx'];

        $ret = self::$queue->sendMessages($array);
        $this->assertCount(3, $ret);

        $received = 0;
        while ($messages = self::$queue->receiveMessages(10)){
            $received += count($messages);
        }
        $this->assertEquals(3, $received);


        self::$queue->deleteMessages($messages);
        $messages = self::$queue->receiveMessages(10);
        $this->assertCount(0, $messages);

    }

    public function atestTopic()
    {

        self::$topid->publish("subject", "body");
        $this->assertEquals(0, 0);
    }
}
