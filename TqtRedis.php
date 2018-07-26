<?php
/**
 * Created by PhpStorm.
 * User: chenyuan3
 * Date: 2018/6/14
 * Time: 下午3:01
 */

namespace lib\redis;

use common\TqtLog;
use config\RedisConf;

/**
 * 此redis操作类，支持主从的redis操作
 * 所有的写操作全部写入主库，读操作读取从库
 *
 * ！！！！！！注意！！！！！！，为了最大限度的保证主从同步，在发生主库操作后，此类会优先使用主库读写。
 * ！！！！！！注意！！！！！！，redis主从同步具有延迟性。一定要注意先读后写的数据出现错误
 *
 *
 * @package lib\redis]
 */
class TqtRedis
{
    private $redisName;
    private $redisRole;
    private $link;
    private static $LINK_POOL = array();

    private function __construct($redisName)
    {
        $this->redisName = $redisName;
    }

    public static function getRedis($redisName)
    {
        return new self($redisName);
    }

    /**
     * @param $methodName
     * @param $arguments
     * @return mixed
     * @throws \Exception
     */
    public function __call($methodName, $arguments)
    {
        $this->init($methodName);
        try {
            return call_user_func_array(array($this, $methodName), $arguments);
        } catch (\Exception $e) {
            TqtLog::error('redis', 'redis error!', array($e, $this->getRedisConf()));
            return false;
        }
    }


    private function setTNX($keyName, $value, $expireTime)
    {
        $this->expireTime($keyName, $expireTime);
        return $this->link->SETNX($keyName, $value);
    }

    /**
     * @param $keyName
     * @return mixed
     */
    private function get($keyName)
    {
        return $this->link->GET($keyName);
    }

    /**
     * @param $keyName
     * @param $value
     * @param $expireTime
     * @return bool
     */
    private function set($keyName, $value, $expireTime)
    {
        if (empty($expireTime)) {
            return false;
        }
        $this->expireTime($keyName, $expireTime);
        return $this->link->SET($keyName, $value);
    }

    /**
     * @param $keyName
     * @param $expireTime
     * @return bool
     */
    private function incr($keyName, $expireTime)
    {
        if (empty($expireTime)) {
            return false;
        }

        $this->expireTime($keyName, $expireTime);
        return $this->link->INCR($keyName);
    }

    /**
     * @param $keyName
     * @param $step
     * @param $expireTime
     * @return bool
     */
    private function incrByStep($keyName, $step, $expireTime)
    {
        if (empty($expireTime)) {
            return false;
        }

        $this->expireTime($keyName, $expireTime);
        return $this->link->INCRBY($keyName, $step);
    }

    /**
     * @param      $keyName
     * @param      $data
     * @param bool $expireTime
     * @return mixed
     */
    private function push($keyName, $data, $expireTime = false)
    {
        if ($expireTime) {
            $this->expireTime($keyName, $expireTime);
        }

        return $this->lpush($keyName, $data);
    }

    private function lpush($keyName, $data)
    {
        return $this->link->LPUSH($keyName, $data);
    }

    /**
     * @param $keyName
     * @return mixed
     */
    private function pop($keyName)
    {
        return $this->rpop($keyName);
    }

    private function rpop($keyName)
    {
        return $this->link->RPOP($keyName);
    }

    private function listLen($keyName)
    {
        return $this->llen($keyName);
    }

    private function llen($keyName)
    {
        return $this->link->LLEN($keyName);
    }

    private function del($keyName)
    {
        return $this->link->DEL($keyName);
    }

    /**
     * @param $keyName
     * @param $expireTime
     * @return mixed
     */
    private function expireTime($keyName, $expireTime)
    {
        return $this->link->EXPIRE($keyName, $expireTime);
    }

    /**
     * @param $methodName
     * @throws \Exception
     */
    private function init($methodName)
    {
        $this->redisRole = $this->setRedisRole($methodName);
        $this->link = $this->getLink();
    }

    /**
     * 设置操作redis的角色
     *
     * @param $methodName
     * @return  string
     */
    private function setRedisRole($methodName)
    {
        if ($this->isReadOperation($methodName)) {
            return 'slave';
        } else {
            return 'master';
        }
    }

    /**
     * @return mixed
     * @throws \Exception
     */
    private function getRedisConf()
    {
        if ($this->redisRole == 'slave') {
            return $this->getSlaveConf();
        }

        return $this->getMasterConf();
    }

    /**
     * @return mixed
     * @throws \Exception
     */
    private function getMasterConf()
    {
        return RedisConf::getDbConf()->getServerConf($this->redisName, "master");
    }

    /**
     * @return mixed
     * @throws \Exception
     */
    private function getSlaveConf()
    {
        return RedisConf::getDbConf()->getServerConf($this->redisName, "slave");
    }

    /**
     * 确定是否是只读方法
     *
     * @param $methodName
     * @return bool
     */
    private function isReadOperation($methodName)
    {
        $readMethodList = array(
            'TYPE', 'KEYS', 'SCAN', 'RANDOMKEY', 'GET', 'MGET', 'SUBSTR', 'STRLEN', 'GETRANGE', 'GETBIT', 'LLEN', 'LRANGE', 'LINDEX', 'SCARD', 'SISMEMBER', 'SINTER', 'SUNION', 'SDIFF', 'SMEMBERS', 'SSCAN', 'SRANDMEMBER', 'ZRANGE', 'ZREVRANGE', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE', 'ZCARD', 'ZSCORE', 'ZCOUNT', 'ZRANK', 'ZREVRANK', 'ZSCAN', 'ZLEXCOUNT', 'ZRANGEBYLEX', 'ZREVRANGEBYLEX', 'HGET', 'HMGET', 'HLEN', 'HKEYS', 'HVALS', 'HGETALL', 'HSCAN', 'HSTRLEN', 'AUTH', 'SELECT', 'ECHO', 'QUIT', 'OBJECT', 'BITCOUNT', 'BITPOS', 'TIME', 'PFCOUNT', 'SORT', 'BITFIELD', 'GEOHASH', 'GEOPOS', 'GEODIST', 'GEORADIUS', 'GEORADIUSBYMEMBER'
        );
        return in_array(strtoupper($methodName), $readMethodList);
    }

    /**
     * 1.优先去主库连接，有主库连接直接返回
     * 2.没有主库连接，看连接类型 建立连接
     *
     * @return string
     * @throws \Exception
     */
    private function getLink()
    {
        if ($this->getMasterLink() !== false) {
            return $this->getMasterLink();
        }

        $link = $this->getLinkFromPool();
        if (false == $this->isAvailable($link)) {
            $link = $this->createLink();
            self::$LINK_POOL[ $this->redisName ][ $this->redisRole ] = $link;
        }

        return $link;
    }

    /**
     * @return bool|mixed
     */
    private function getLinkFromPool()
    {
        if (false == array_key_exists($this->redisName, self::$LINK_POOL)) {
            return false;
        }

        return array_key_exists($this->redisRole, self::$LINK_POOL[ $this->redisName ]) ? self::$LINK_POOL[ $this->redisName ] : false;
    }

    /**
     * @return \Redis
     * @throws \Exception
     */
    private function createLink()
    {
        $conf = $this->getRedisConf();
        $link = new \Redis();
        $link->connect($conf['host'], $conf['port'], 2);
        self::$LINK_POOL[ $this->redisName ][ $this->redisRole ] = $link;

        return $link;
    }

    /**
     * 获取主库连接
     *
     * @return bool
     */
    private function getMasterLink()
    {
        if (false == array_key_exists($this->redisName, self::$LINK_POOL) || false == array_key_exists("master", self::$LINK_POOL[ $this->redisName ])) {
            return false;
        }

        $masterLink = self::$LINK_POOL[ $this->redisName ]['master'];
        if (false == $this->isAvailable($masterLink)) {
            return false;
        }

        return $masterLink;
    }

    /**
     * @param $link
     * @return bool
     */
    private function isAvailable($link)
    {
        if ($link instanceof \Redis && '+PONG' == $link->ping()) {
            return true;
        }

        return false;
    }
}