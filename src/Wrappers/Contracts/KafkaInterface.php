<?php

namespace Oasis\Mlib\AliyunWrappers\Contracts;

interface KafkaInterface
{

    public function putSingleRecord($data);
    public function putBatchRecord($data);
    public function putBatchRecordAsync($data);
}
