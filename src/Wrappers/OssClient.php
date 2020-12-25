<?php

namespace Oasis\Mlib\AliyunWrappers;

class OssClient extends \OSS\OssClient
{
    /** @noinspection PhpUnusedParameterInspection */
    public function getPresignedUri($path, $expires = '+30 minutes')
    {
        throw new \Exception("No implement for getPresignedUri()");
    }
}
