<?php

namespace Oasis\Mlib\AliyunWrappers\Test;

use League\Flysystem\FileNotFoundException;
use Oasis\Mlib\AliyunWrappers\ExtendedAliyunOssAdapter;
use Oasis\Mlib\FlysystemWrappers\ExtendedFilesystem;
use Oasis\Mlib\Utils\ArrayDataProvider;
use Oasis\Mlib\Utils\DataProviderInterface;
use OSS\Core\OssException;
use OSS\OssClient;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class OssAdapterTest extends TestCase
{

    static $topid;
    static $efs;

    public static function setUpBeforeClass()
    {

        parent::setUpBeforeClass();

        $dataSourceFile = __DIR__ . "/config.yml";
        $config         = Yaml::parse(file_get_contents($dataSourceFile));
        $dp             = new ArrayDataProvider($config);
        $config         = $dp->getMandatory('mns', DataProviderInterface::ARRAY_TYPE);

        try {
            $ossClient = new OssClient($config['access_key'], $config['access_secret'], $config['oss_end_point']);
        }
        catch (OssException $e) {
        }

        $adapter   = new ExtendedAliyunOssAdapter($ossClient, $config['bucket']);
        self::$efs = new ExtendedFilesystem($adapter);
    }

    public function testOssAdapter()
    {

        self::$efs->put("test",'x');
        $ret = self::$efs->has("test");
        $this->assertTrue($ret);

        try {
            self::$efs->delete("test");
        }
        catch (FileNotFoundException $e) {
        }
        $ret = self::$efs->has("test");
        $this->assertNotTrue($ret);
    }


}
