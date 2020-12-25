<?php

namespace Oasis\Mlib\AliyunAdb;


use Oasis\Mlib\Redshift\CredentialProviderInterface;

class OssCredentialProvider implements CredentialProviderInterface
{

    private $accessKeyId;
    private $accessKeySecret;
    public  $endPoint;

    public function __construct($accessKeyId, $accessKeySecret, $endPoint)
    {

        $this->accessKeyId     = $accessKeyId;
        $this->accessKeySecret = $accessKeySecret;
        $this->endPoint        = $endPoint;
    }

    public function getCredentialString()
    {

        return [
            "access_key"    => $this->accessKeyId,
            "access_secret" => $this->accessKeySecret,
            "end_point"     => $this->endPoint];
    }
}
