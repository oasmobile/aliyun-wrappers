<?php

namespace Oasis\Mlib\AliyunWrappers;

use InvalidArgumentException;
use OSS\Core\OssException;

class OssClient extends \OSS\OssClient
{
    
    
    public function getPresignedUri($path, $expires = '+30 minutes')
    {
        $path = $this->normalizeFilePath($path);
        
        if (preg_match('#^oss://(.*?)/(.*)$#', $path, $matches)) {
            $bucket = $matches[1];
            $path   = $matches[2];
        }
        else {
            throw new InvalidArgumentException("path should be a full path starting with oss://");
        }
        
        $timeout = is_string($expires) ? strtotime($expires) - time() : $expires;
        
        try {
            return $this->signUrl($bucket, $path, $timeout);
        }
        catch (OssException $e) {
        }
        
        return  false;
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
    private function stringStartsWith($haystack, $needle)
    {
        
        return
            $needle === ""
            || strrpos($haystack, $needle, -strlen($haystack)) !== false;
    }
}
