<?php

namespace Oasis\Mlib\AliyunWrappers;

use Aliyun_Log_Client;
use Aliyun_Log_Models_LogItem;
use Aliyun_Log_Models_PutLogsRequest;
use GuzzleHttp\Promise\Promise;

//todo: add interface
class AliyunSLS extends Aliyun_Log_Client
{

    public $project;
    public $logstore;
    public $topic;

    public function __construct($accessKeyId, $accessKey, $endpoint, $project, $logstore)
    {

        $this->project  = $project;
        $this->logstore = $logstore;
        parent::__construct($endpoint, $accessKeyId, $accessKey);
    }

    public function putSingleRecord($data)
    {

        $logItem = new Aliyun_Log_Models_LogItem();
        $logItem->setTime(time());
        $logItem->setContents(['data' => $data]);

        $request  = new Aliyun_Log_Models_PutLogsRequest(
            $this->project,
            $this->logstore,
            null,
            null,
            [$logItem]
        );
        $response = $this->putLogs($request);

        return $response->getRequestId();
    }

    public function putBatchRecord($data)
    {

        $logItems = [];
        foreach ($data as $item) {
            $logItem = new Aliyun_Log_Models_LogItem();
            $logItem->setTime(time());
            $logItem->setContents(['data' => $item]);
            $logItems[] = $logItem;
        }

        $request = new Aliyun_Log_Models_PutLogsRequest(
            $this->project,
            $this->logstore,
            null,
            null,
            $logItems
        );

        $response = $this->putLogs($request);

        return $response->getRequestId();
    }

    public function putBatchRecordAsync($data)
    {

        $promise = new Promise();
        $promise->resolve($this->putBatchRecord($data));

        return $promise;
    }
}
