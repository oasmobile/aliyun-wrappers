<?php

namespace Oasis\Mlib\AliyunAdb;

use Oasis\Mlib\Redshift\CredentialProviderInterface;
use Oasis\Mlib\Redshift\Drivers\ConnectionAnalyticExtension;
use OSS\OssClient;

class AliyunAdbAnalyticExtension extends ConnectionAnalyticExtension
{

    public function copyFromS3(
        $table,
        $columns,
        $filePath,
        $region,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $maxerror = 0,
        $options = []
    )
    {

        $fileList   = [];
        $credential = $credentialProvider->getCredentialString();

        if (in_array('MANIFEST', $options)) {


            $client = new OssClient($credential['access_key'], $credential['access_secret'], $credential['end_point']);

            list($bucket, $object) = $this->parseOssPath($filePath);
            $content = $client->getObject($bucket, $object);
            $list    = \GuzzleHttp\json_decode($content, true);

            if ($list) {
                foreach ($list['entries'] as $item) {
                    $fileList[] = $item['url'];
                }
            }
        }
        else {
            $fileList[] = $filePath;
        }

        foreach ($fileList as $path) {

            $stmt = sprintf(
                "COPY %s (%s) FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s'  format  csv %s %s %s",
                $this->connection->normalizeTable($table),
                $this->connection->normalizeColumns($columns),
                $this->normalizeFilePath($path),
                $credential['access_key'],
                $credential['access_secret'],
                ($gzip ? " filetype 'gzip'" : ""),
                ($escaped ? " \"escape\" '\'" : ""),
                ($maxerror > 0 ? "segment_reject_limit '$maxerror'" : "")
            );
            if (!in_array('delimiter', $options)) {
                $stmt .= " \"delimiter\" '|' ";
            }

            $stmt .= sprintf(" ENDPOINT '%s' FDW 'oss_fdw'", $credential['end_point']);

            $prepared_statement = $this->connection->prepare($stmt);
            $prepared_statement->execute();
        }
    }

    public function unloadToS3(
        $sql,
        $filePath,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $parallel = true,
        $options = []
    )
    {

        $credential = $credentialProvider->getCredentialString();
        $stmt       = sprintf(
            "UNLOAD (%s) TO '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s'  format text  %s %s ",
            $this->connection->normalizeSingleQuotedValue($sql),
            $this->normalizeFilePath($filePath),
            $credential['access_key'],
            $credential['access_secret'],
            ($gzip ? " filetype 'gzip'" : ""),
            ($escaped ? " \"escape\" '\'" : "")
        );

        if (in_array('ADDQUOTES', $options)) {
            $stmt .= " \"quote\" '\"' force_quote_all 'true' ";
            unset($options['ADDQUOTES']);
        }

        if (!in_array('delimiter', $options)) {
            $stmt .= " \"delimiter\" '|' ";
        }
        $stmt .= sprintf(" ENDPOINT '%s' FDW 'oss_fdw'", $credential['end_point']);

        $prepared_statement = $this->connection->prepare($stmt);
        $prepared_statement->execute();

        if ($parallel == false) {

            $client = new OssClient($credential['access_key'], $credential['access_secret'], $credential['end_point']);
            list($bucket, $object) = $this->parseOssPath($filePath);
            $list = $client->listObjects($bucket, ['prefix' => $object]);

            $content = '';
            foreach ($list->getObjectList() as $item) {

                $content .= $client->getObject($bucket, $item->getKey());
                $client->deleteObject($bucket, $item->getKey());
            }
            $ext = "000" . ($gzip == true ? ".gz" : "");
            $client->putObject($bucket, $object . $ext, $content);
        }
    }

    private function normalizeFilePath($path)
    {

        static $protocol = "oss://";
        if ($this->stringStartsWith($path, $protocol)) {
            $path = substr($path, strlen($protocol));
        }
        $path = preg_replace('#/+#', "/", $path);

        return $protocol . $path;
    }

    private function parseOssPath($path)
    {

        $path   = $this->normalizeFilePath($path);
        $str    = substr($path, strlen('oss://'));
        $bucket = substr($str, 0, strpos($str, '/'));
        $object = substr($str, strpos($str, '/') + 1);

        return [$bucket, $object];
    }

    private function stringStartsWith($haystack, $needle)
    {

        return
            $needle === ""
            || strrpos($haystack, $needle, -strlen($haystack)) !== false;
    }
}
