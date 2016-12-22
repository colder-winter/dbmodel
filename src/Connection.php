<?php
/**
 * *********************************************************************************
 * xphp框架，简单易用的微型PHP框架
 * xphp框架PDO数据库驱动
 * -------------------------------------------------------------------------------
 * CopyRight By Sven & 秋士悲
 * 您可以自由使用该源码，但是在使用过程中，请保留作者信息。
 */

namespace Xphp\DbLibrary;

use PDO;
use PDOException;

class Connection
{
    /**
     * PDO数据连接实例
     *
     * @var PDO
     */
    private $pdo;

    /**
     * 当前执行的SQL语句
     *
     * @var string
     */
    private $sql = '';

    /**
     * 类构造函数，连接PDO，生成PDO实例
     *
     * @param array   $dbConfig 数据库连接配置
     *
     * @throws \PDOException
     */
    public function __construct($dbConfig)
    {
        // 判断服务器环境是否支持PDO
        if (!class_exists('PDO')) {
            throw new PDOException("当前服务器环境不支持PDO，访问数据库失败。", 3001);
        }

        // 判断是传入了正确的数据库配置参数
        if (empty($dbConfig['host'])) {
            throw new PDOException("没有定义数据库配置，请在配置文件中配置。", 3002);
        }

        //字符集
        $names = (isset($dbConfig['charset']) && !empty($dbConfig['charset'])) ? $dbConfig['charset'] : 'utf8';

        //生成数据库配置
        $dbConfig['dsn'] = sprintf("%s:host=%s;port=%s;dbname=%s",
            $dbConfig['driver'],
            $dbConfig['host'],
            $dbConfig['port'],
            $dbConfig['dbname']
        );

        $dbConfig['params'] = [
            PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES {$names}",   //设置字符集
            PDO::ATTR_CASE => PDO::CASE_NATURAL,                    //返回列名的大小写模式采用自然模式
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,            //PDO错误模式设为抛出异常
            PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,             //使用查询缓存
            PDO::ATTR_PERSISTENT => false,                          //持久连接
            PDO::ATTR_TIMEOUT => 10,                                //查询超时10秒
            PDO::ATTR_EMULATE_PREPARES => false,                    //启用或禁用预处理语句的模拟。
            PDO::ATTR_STRINGIFY_FETCHES => false                    //以字符串输出数字。
        ];

        //连接数据库，生成PDO实例
        try {
            $this->pdo = new PDO($dbConfig['dsn'], $dbConfig['user'], $dbConfig['passwd'], $dbConfig['params']);
        } catch (PDOException $e) {
            throw new PDOException($e->getMessage(), 3003);
        }

        if (!$this->pdo) {
            throw new PDOException('PDO CONNECT ERROR', 3005);
        }
    }

    /**
     * 生成PDOStatement并执行数据库查询语句，绑定参数
     *
     * @param string $sql 要执行的SQL语句。
     * @param array  $params 要绑定的数据
     *
     * @return \PDOStatement
     */
    private function execute($sql = '', $params = [])
    {
        if (empty(trim($sql))) {
            throw new PDOException('要执行的SQL语句为空。', 3006);
        }

        $this->sql = $sql;

        $pdoStatement = $this->pdo->prepare($sql);
        if (empty($pdoStatement)) {
            throw new PDOException('PDO执行失败', 3007);
        }

        if (is_array($params) && !empty($params)) {
            foreach ($params as $key => $value) {
                $pdoStatement->bindValue(
                    is_string($key) ? $key : $key + 1, $value,
                    is_int($value) || is_float($value) ? PDO::PARAM_INT : PDO::PARAM_STR
                );
            }
        }

        $pdoStatement->execute();

        return $pdoStatement;
    }

    /**
     * 执行一条更新性的SQL语句并返回影响行数
     *
     * @param string $sql 要执行的SQL语句。
     * @param array  $params 绑定参数
     *
     * @return integer 受影响的行数
     */

    public function exec($sql, $params = [])
    {
        $pdoStatement = $this->execute($sql, $params);
        return $pdoStatement->rowCount();
    }

    /**
     * 执行一条查询性的SQL语句并返回结果集
     *
     * @param string $sql 要执行的SQL语句。
     * @param array  $params 绑定参数
     *
     * @return array
     */
    public function query($sql, $params = [])
    {
        $pdoStatement = $this->execute($sql, $params);
        return $pdoStatement->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * 执行查询获取多条数据
     *
     * @param string  $sql 要执行的SQL指令
     * @param array   $params 要绑定的数据
     * @param boolean $primkey 是否以主键为下标。使用主键下标，可以返回以数据库主键的值为下标的二维数组
     *
     * @return array
     */
    public function getRows($sql, $params = [], $primkey = false)
    {
        $pdoStatement = $this->execute($sql, $params);
        if ($primkey) {
            $result = $pdoStatement->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_ASSOC);
        } else {
            $result = $pdoStatement->fetchAll(PDO::FETCH_ASSOC);
        }
        return $result;
    }

    /**
     * 执行查询获取一条数据
     *
     * @param string $sql 要执行的SQL指令
     * @param array  $params 绑定数据
     *
     * @return array|mixed
     */
    public function getRow($sql, $params = [])
    {
        $pdoStatement = $this->execute($sql, $params);
        return $pdoStatement->fetch(PDO::FETCH_ASSOC, PDO::FETCH_ORI_NEXT);
    }

    /**
     * 获取一个字段的值
     *
     * @param string $sql 要执行的SQL指令
     * @param array  $params 绑定数据
     *
     * @return mixed|string
     */
    public function getOne($sql, $params = [])
    {
        $pdoStatement = $this->execute($sql, $params);
        return $pdoStatement->fetchColumn();
    }

    /**
     * 获取最近一次查询的SQL语句
     *
     * @return string
     */
    public function getLastSql()
    {
        return $this->sql;
    }

    /**
     * 获取最后插入的ID
     *
     * @return integer
     */
    public function lastInsertId()
    {
        return $this->pdo->lastInsertId();
    }

    /**
     * 开始一个事务
     */
    public function beginTransaction()
    {
        return $this->pdo->beginTransaction();
    }

    /**
     * 回滚一个事务
     */
    public function rollBack()
    {
        return $this->pdo->rollBack();
    }

    /**
     * 回滚一个事务
     */
    public function commit()
    {
        return $this->pdo->commit();
    }

    /**
     * 关闭数据库PDO连接
     */
    public function close()
    {
        $this->pdo = null;
    }
}

