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

        $credential = $credentialProvider->getCredentialString();
        $stmt       = sprintf(
            "COPY %s (%s) FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' format csv %s %s %s %s %s",
            $this->connection->normalizeTable($table),
            $this->connection->normalizeColumns($columns),
            $this->normalizeFilePath($filePath),
            $credential['access_key'],
            $credential['access_secret'],
            in_array('MANIFEST', $options) ? "MANIFEST" : "",
            ($gzip ? " filetype 'gzip'" : ""),
            ($escaped ? " \"escape\" '\'" : ""),
            ($maxerror > 0 ? "segment_reject_limit '$maxerror'" : ""),
            !in_array('delimiter', $options) ? " \"delimiter\" '|' " : ""
        );

        $stmt .= sprintf(" ENDPOINT '%s' FDW 'oss_fdw'", $credential['end_point']);

        mdebug("Copying using stmt:\n%s", $stmt);
        $this->connection->exec($stmt);
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

        $manifest   = $filePath . ".manifest";
        $credential = $credentialProvider->getCredentialString();
        $stmt       = sprintf(
            "UNLOAD (%s) TO '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s'  format text  %s %s %s %s %s ALLOWOVERWRITE 'true' \"null\" '' ",
            $this->normalizeSingleQuotedValue($sql),
            $this->normalizeFilePath($filePath),
            $credential['access_key'],
            $credential['access_secret'],
            ($parallel ? "" : "MANIFEST '" . $manifest . "' PARALLEL OFF"),
            ($gzip ? " filetype 'gzip'" : ""),
            ($escaped ? " \"escape\" '\'" : ""),
            !in_array('delimiter', $options) ? " \"delimiter\" '|' " : "",
            in_array('ADDQUOTES', $options) ? " \"quote\" '\"' force_quote_all 'true' " : ""
        );

        $stmt .= sprintf(" ENDPOINT '%s' FDW 'oss_fdw'", $credential['end_point']);

        mdebug("Unloading using stmt:\n%s", $stmt);
        $this->connection->exec($stmt);

        if ($parallel == false) {

            $client = new OssClient($credential['access_key'], $credential['access_secret'], $credential['end_point']);
            list($bucket, $object) = $this->parseOssPath($manifest);
            $content = $client->getObject($bucket, $object);
            $content = json_decode($content, true);


            if (isset($content['entries'][0]['url'])) {
                list($bucket, $fromObject) = $this->parseOssPath($content['entries'][0]['url']);
                list($bucket, $toObject) = $this->parseOssPath($filePath);
                $ext      = "000" . ($gzip == true ? ".gz" : "");
                $toObject = $toObject . $ext;

                $client->copyObject($bucket, $fromObject, $bucket, $toObject);
            }
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

    private function normalizeSingleQuotedValue($value)
    {

        $value = pg_escape_string($value);

        return "'$value'";
    }
}
