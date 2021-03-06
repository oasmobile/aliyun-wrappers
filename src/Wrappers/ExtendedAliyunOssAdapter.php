<?php

namespace Oasis\Mlib\AliyunWrappers;

use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Adapter\Polyfill\StreamedTrait;
use League\Flysystem\Config;
use League\Flysystem\Util;
use Oasis\Mlib\FlysystemWrappers\FindableAdapterInterface;
use OSS\Core\OssException;
use OSS\OssClient;
use Symfony\Component\Finder\Finder;

class ExtendedAliyunOssAdapter extends AbstractAdapter implements FindableAdapterInterface
{

    use StreamedTrait;
    use NotSupportingVisibilityTrait;

    /**
     * Aliyun Oss Client.
     *
     * @var OssClient
     */
    protected $client;

    /**
     * bucket name.
     *
     * @var string
     */
    protected $bucket;

    /**
     * @var array
     */
    protected $options = [];

    /**
     * @var array
     */
    protected static $mappingOptions = [
        'mimetype' => OssClient::OSS_CONTENT_TYPE,
        'size'     => OssClient::OSS_LENGTH,
        'filename' => OssClient::OSS_CONTENT_DISPOSTION,
    ];

    /**
     * protocol => registering adapter
     *
     * @var array
     */
    protected static $registeredWrappers = [];

    /**
     * ExtendedAliyunOssAdapter constructor.
     *
     * @param OssClient      $client
     * @param                $bucket
     * @param null           $prefix
     * @param array          $options
     */
    public function __construct(OssClient $client, $bucket, $prefix = null, array $options = [])
    {

        $this->client = $client;
        $this->bucket = $bucket;
        $this->setPathPrefix($prefix);
        $this->options = array_merge($this->options, $options);
    }

    /**
     * Get the Aliyun Oss Client bucket.
     *
     * @return string
     */
    public function getBucket()
    {

        return $this->bucket;
    }

    /**
     * Get the Aliyun Oss Client instance.
     *
     * @return OssClient
     */
    public function getClient()
    {

        return $this->client;
    }

    /**
     * @param                          $path
     * @param                          $localFilePath
     * @param Config                   $config
     *
     * @return array|false
     */
    public function putFile($path, $localFilePath, Config $config)
    {

        $object  = $this->applyPathPrefix($path);
        $options = $this->getOptionsFromConfig($config);

        $options[OssClient::OSS_CHECK_MD5] = true;

        if (!isset($options[OssClient::OSS_CONTENT_TYPE])) {
            $options[OssClient::OSS_CONTENT_TYPE] = Util::guessMimeType($path, '');
        }

        try {
            $this->client->uploadFile($this->bucket, $object, $localFilePath, $options);
        }
        catch (OssException $e) {
            return false;
        }

        $type               = 'file';
        $result             = compact('type', 'path');
        $result['mimetype'] = $options[OssClient::OSS_CONTENT_TYPE];

        return $result;
    }

    /**
     * @param string                   @$path
     * @param string                   @$contents
     * @param Config $config
     *
     * @return array|false
     */
    public function write($path, $contents, Config $config)
    {

        $object  = $this->applyPathPrefix($path);
        $options = $this->getOptionsFromConfig($config);

        if (!isset($options[OssClient::OSS_LENGTH])) {
            $options[OssClient::OSS_LENGTH] = Util::contentSize($contents);
        }

        if (!isset($options[OssClient::OSS_CONTENT_TYPE])) {
            $options[OssClient::OSS_CONTENT_TYPE] = Util::guessMimeType($path, $contents);
        }

        $this->client->putObject($this->bucket, $object, $contents, $options);

        $type               = 'file';
        $result             = compact('type', 'path', 'contents');
        $result['mimetype'] = $options[OssClient::OSS_CONTENT_TYPE];
        $result['size']     = $options[OssClient::OSS_LENGTH];

        return $result;
    }

    /**
     * Update a file.
     *
     * @param string @$path
     * @param string @$contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config)
    {

        return $this->write($path, $contents, $config);
    }

    /**
     * Rename a file.
     *
     * @param string @$path
     * @param string @$newpath
     *
     * @return bool
     */
    public function rename($path, $newpath)
    {

        if (!$this->copy($path, $newpath)) {
            return false;
        }

        return $this->delete($path);
    }

    /**
     * Copy a file.
     *
     * @param string @$path
     * @param string @$newpath
     *
     * @return bool
     */
    public function copy($path, $newpath)
    {

        $object    = $this->applyPathPrefix($path);
        $newobject = $this->applyPathPrefix($newpath);

        try {
            $this->client->copyObject($this->bucket, $object, $this->bucket, $newobject);
        }
        catch (OssException $e) {
            return false;
        }

        return true;
    }

    /**
     * Delete a file.
     *
     * @param string @$path
     *
     * @return bool
     */
    public function delete($path)
    {

        $object = $this->applyPathPrefix($path);

        $this->client->deleteObject($this->bucket, $object);

        return true;
    }

    /**
     * Delete a directory.
     *
     * @param string @$dirname
     *
     * @return bool
     * @throws OssException
     */
    public function deleteDir($dirname)
    {

        $list = $this->listContents($dirname, true);

        $objects = [];
        foreach ($list as $val) {
            if ($val['type'] === 'file') {
                $objects[] = $this->applyPathPrefix($val['path']);
            }
            else {
                $objects[] = $this->applyPathPrefix($val['path']) . '/';
            }
        }

        $this->client->deleteObjects($this->bucket, $objects);

        return true;
    }

    /**
     * Create a directory.
     *
     * @param string @$dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config)
    {

        $object  = $this->applyPathPrefix($dirname);
        $options = $this->getOptionsFromConfig($config);

        $this->client->createObjectDir($this->bucket, $object, $options);

        return ['path' => $dirname, 'type' => 'dir'];
    }

    /**
     * Check whether a file exists.
     *
     * @param string @$path
     *
     * @return bool
     */
    public function has($path)
    {

        $object = $this->applyPathPrefix($path);

        if ($this->client->doesObjectExist($this->bucket, $object)) {
            return true;
        }
        $list = $this->listContents($path);
        if (count($list) > 0) {
            return true;
        }

        return false;
    }

    /**
     * Read a file.
     *
     * @param string @$path
     *
     * @return array|false
     */
    public function read($path)
    {

        $object = $this->applyPathPrefix($path);

        $contents = $this->client->getObject($this->bucket, $object);

        return compact('contents', 'path');
    }

    public function readStream($path)
    {
        $object    = $this->applyPathPrefix($path);
        $stream    = tmpfile();
        $resource  = stream_get_meta_data($stream);
        $localfile = $resource['uri'];

        $option = [
            OssClient::OSS_FILE_DOWNLOAD => $localfile,
        ];
        $this->client->getObject($this->bucket, $object, $option);

        return compact('stream', 'path');
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool   $recursive
     *
     * @return array
     * @throws OssException
     */
    public function listContents($directory = '', $recursive = false)
    {

        $directory = rtrim($this->applyPathPrefix($directory), '\\/');
        if ($directory) $directory .= '/';

        $bucket     = $this->bucket;
        $delimiter  = '/';
        $nextMarker = '';
        $maxkeys    = 1000;
        $options    = [
            'delimiter' => $delimiter,
            'prefix'    => $directory,
            'max-keys'  => $maxkeys,
            'marker'    => $nextMarker,
        ];

        $listObjectInfo = $this->client->listObjects($bucket, $options);

        $objectList = $listObjectInfo->getObjectList(); // 文件列表
        $prefixList = $listObjectInfo->getPrefixList(); // 目录列表

        $result = [];
        foreach ($objectList as $objectInfo) {
            if ($objectInfo->getSize() === 0 && $directory === $objectInfo->getKey()) {
                $result[] = [
                    'type'      => 'dir',
                    'path'      => $this->removePathPrefix(rtrim($objectInfo->getKey(), '/')),
                    'timestamp' => strtotime($objectInfo->getLastModified()),
                ];
                continue;
            }

            $result[] = [
                'type'      => 'file',
                'path'      => $this->removePathPrefix($objectInfo->getKey()),
                'timestamp' => strtotime($objectInfo->getLastModified()),
                'size'      => $objectInfo->getSize(),
            ];
        }

        foreach ($prefixList as $prefixInfo) {
            if ($recursive) {
                $next   = $this->listContents($this->removePathPrefix($prefixInfo->getPrefix()), $recursive);
                $result = array_merge($result, $next);
            }
            else {
                $result[] = [
                    'type'      => 'dir',
                    'path'      => $this->removePathPrefix(rtrim($prefixInfo->getPrefix(), '/')),
                    'timestamp' => 0,
                ];
            }
        }

        return $result;
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string @$path
     *
     * @return array|false
     */
    public function getMetadata($path)
    {

        $object = $this->applyPathPrefix($path);

        $result = $this->client->getObjectMeta($this->bucket, $object);

        return [
            'type'      => 'file',
            'dirname'   => Util::dirname($path),
            'path'      => $path,
            'timestamp' => strtotime($result['last-modified']),
            'mimetype'  => $result['content-type'],
            'size'      => $result['content-length'],
        ];
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string @$path
     *
     * @return array|false
     */
    public function getSize($path)
    {

        return $this->getMetadata($path);
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string @$path
     *
     * @return array|false
     */
    public function getMimetype($path)
    {

        return $this->getMetadata($path);
    }

    /**
     * Get the timestamp of a file.
     *
     * @param string @$path
     *
     * @return array|false
     */
    public function getTimestamp($path)
    {

        return $this->getMetadata($path);
    }

    /**
     * Get the signed download url of a file.
     *
     * @param string @$path
     * @param int    $expires
     * @param string $host_name
     * @param bool   $use_ssl
     *
     * @return string
     * @throws OssException
     */
    public function getSignedDownloadUrl($path, $expires = 3600, $host_name = '', $use_ssl = false)
    {

        $object = $this->applyPathPrefix($path);
        $url    = $this->client->signUrl($this->bucket, $object, $expires);

        if (!empty($host_name) || $use_ssl) {
            $parse_url = parse_url($url);
            if (!empty($host_name)) {
                $parse_url['host'] = $this->bucket . '.' . $host_name;
            }
            if ($use_ssl) {
                $parse_url['scheme'] = 'https';
            }

            $url = (isset($parse_url['scheme']) ? $parse_url['scheme'] . '://' : '')
                   . (
                   isset($parse_url['user']) ?
                       $parse_url['user'] . (isset($parse_url['pass']) ? ':' . $parse_url['pass'] : '') . '@'
                       : ''
                   )
                   . (isset($parse_url['host']) ? $parse_url['host'] : '')
                   . (isset($parse_url['port']) ? ':' . $parse_url['port'] : '')
                   . (isset($parse_url['path']) ? $parse_url['path'] : '')
                   . (isset($parse_url['query']) ? '?' . $parse_url['query'] : '');
        }

        return $url;
    }

    /**
     * Get options from the config.
     *
     * @param Config $config
     *
     * @return array
     */
    protected function getOptionsFromConfig(Config $config)
    {

        $options = $this->options;
        foreach (static::$mappingOptions as $option => $ossOption) {
            if (!$config->has($option)) {
                continue;
            }
            $options[$ossOption] = $config->get($option);
        }

        return $options;
    }

    public function getRealpath($path)
    {

        $path = $this->applyPathPrefix($path);

        return sprintf("oss://%s/%s", $this->getBucket(), $path);
    }

    public function getFinder($path = '')
    {

        if (($protocol = array_search($this, self::$registeredWrappers))
            === false
        ) {
            $protocol = $this->registerStreamWrapper(null);
        }

        $path   = sprintf(
            "%s://%s/%s",
            $protocol,
            $this->getBucket(),
            $this->applyPathPrefix($path)
        );
        $finder = new Finder();
        $finder->in($path);

        return $finder;
    }

    /** @noinspection PhpUnusedParameterInspection */
    public function registerStreamWrapper($protocol = "s3")
    {

        throw new \Exception("Not implement yet: registerStreamWrapper()");
    }
}
