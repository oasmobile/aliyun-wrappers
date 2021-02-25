<?php

namespace Oasis\Mlib\AliyunWrappers\Test;

use Oasis\Mlib\AliyunAdb\AliyunAdbAnalyticExtension;
use Oasis\Mlib\AliyunAdb\OssCredentialProvider;
use Oasis\Mlib\Redshift\RedshiftConnection;
use Oasis\Mlib\Utils\ArrayDataProvider;
use Oasis\Mlib\Utils\DataProviderInterface;
use OSS\OssClient;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class AdbTest extends TestCase
{

    const FIELDS = [
        "a1",
        "a2",
        "a3",
        "a4",
        "a5",
        "a6",
        "a7",
    ];

    protected static $s3Fs;
    protected static $s3Region;
    /** @var  StsCredentialProvider */
    protected static $sts;
    /** @var  AnalyticDBAwsRedshiftConnection */
    protected static $adb;
    protected static $client;
    protected static $bucket;
    protected static $prefix;
    protected static $path;
    protected static $path2;

    public static function setUpBeforeClass()
    {

        parent::setUpBeforeClass();

        $dataSourceFile = __DIR__ . "/config.yml";
        $config         = Yaml::parse(file_get_contents($dataSourceFile));

        $dp = new ArrayDataProvider($config);

        $config = $dp->getMandatory('aliyun', DataProviderInterface::ARRAY_TYPE);

        self::$client = new OssClient($config['access_key'], $config['access_secret'], $config['end_point']);
        self::$bucket = @$config['bucket'];
        self::$sts    =
            new OssCredentialProvider($config['access_key'], $config['access_secret'], $config['end_point']);

        $params                 = $dp->getMandatory('adb', DataProviderInterface::ARRAY_TYPE);
        $params['analytic_ext'] = new AliyunAdbAnalyticExtension();

        self::$adb    = RedshiftConnection::getConnection($params, null, null);
        self::$prefix = time();
        self::$path   = sprintf("oss://%s/test/%s/record.csv", self::$bucket, self::$prefix);

        $dropStmt   = "DROP TABLE IF EXISTS php_adb_test";
        $createStmt =
            "CREATE TABLE php_adb_test(a1 VARCHAR(64),a2 VARCHAR(64),a3 VARCHAR(64),a4 VARCHAR(64),a5 VARCHAR(64),a6 VARCHAR(64),a7 VARCHAR(64))";

        $insertStmt =
            "insert into php_adb_test values ('a1','a2','a3','a4','a5','a6','a7'),('b1','b2','b3','b4','b5','ab6','b7'),('c1','c2','c3','c4','b5','ab6','\b7')";

        self::$adb->exec($dropStmt);
        self::$adb->exec($createStmt);
        self::$adb->exec($insertStmt);
    }

    public function testUnload()
    {

        self::$adb->unloadToS3(
            "select * from php_adb_test",
            self::$path,
            self::$sts,
            true,
            false,
            false
        );

        $this->assertEquals(1, 1);
    }

    public function testCopy()
    {

        self::$adb->copyFromS3(
            "php_adb_test",
            ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7'],
            self::$path.'000',
            self::$s3Region,
            self::$sts,
            true,
            false,
            100000
        );

        $arr = self::$adb->fetchAll('select count(*) as count from php_adb_test ');

        $this->assertEquals($arr[0]['count'], 6);
    }

    public function testCopyManifest()
    {

        self::$adb->copyFromS3(
            "php_adb_test",
            ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7'],
            self::$path.'.manifest',
            self::$s3Region,
            self::$sts,
            true,
            false,
            1000,
            $options = [
                'MANIFEST'
            ]
        );

        $arr = self::$adb->fetchAll('select count(*) as count from php_adb_test ');
        $this->assertEquals($arr[0]['count'], 9);
    }

}
