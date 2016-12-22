<?php

namespace Xphp\DbLibrary;

use Xphp\DbLibrary\Connection;

/**
 *********************************************************************************
 * xphp框架，简单易用的微型PHP框架
 * xphp项目模型基类
 * -------------------------------------------------------------------------------
 * CopyRight By sven & 秋士悲
 * 您可以自由使用该源码，但是在使用过程中，请保留作者信息。
 */
class DbModel
{
    /**
     * 静态保存PDO连接实例的数组，确保对于每个数据库配置的连接为单例
     *
     * @var array
     */
    private static $pdoConnections = [];

    /**
     * PDO连接实例
     *
     * @var Connection
     */
    public $connection;

    /**
     * 表名
     *
     * @var string
     */
    public $table = '';

    /**
     * 主键字段名
     *
     * @var string
     */
    public $primkey = 'id';

    /**
     * 数据库连接配置名
     *
     * @var string
     */
    protected $connectionName = 'default';

    /**
     * 连接表列表
     *
     * @var array
     */
    private $joinTables = [];

    /**
     * 查询条件
     *
     * @var array
     */
    private $conditions = [];

    /**
     * 扩展查询条件子句
     *
     * @var array
     */
    private $appendConditions = [];

    /**
     * 扩展查询条件绑定值列表
     *
     * @var array
     */
    private $appendBindings = [];

    /**
     * GroupBy子句
     *
     * @var array
     */
    private $groupBys = [];

    /**
     * Having子句条件列表
     *
     * @var array
     */
    private $havings = [];

    /**
     * DbModel constructor.
     *
     * @param bool $persistent 是否保持连接
     *
     * @throws \Exception
     */
    public function __construct()
    {
        //猜测表名
        //类名默认是：表名+Model的首字母大写驼峰命名，取类名去掉Model，然后小写就是表名
        if (empty($this->table)) {
            $this->table = $this->guessTableName();
        }

        //默认主键
        if (empty($this->primkey)) {
            $this->primkey = 'id';
        }

        //创建PDO驱动实例，PDO连接数据库
        $connectionName = $this->connectionName;
        if (!isset(self::$pdoConnections[$connectionName]) || empty(self::$pdoConnections[$connectionName])) {
            $dbConfig = getConfigValue('database', 'database');
            $dbConfig = isset($dbConfig[$connectionName]) ? $dbConfig[$connectionName] : null;
            if (!$dbConfig) {
                $msg = '数据库配置错误';
                throw new \Exception($msg, 500);
            }
            $this->connection = self::$pdoConnections[$connectionName] = new Connection($dbConfig);
        }
    }

    /**
     * 添加连表
     *
     * @param string $table 要连的表
     * @param string $one 表一的关联字段
     * @param string $operator 操作符
     * @param string $two 表二的关联字段
     * @param string $type 连接类型
     *
     * @return $this
     */
    public function joinTable($table, $one, $operator, $two, $type = 'left')
    {
        $joinString = $type . ' JOIN ' . $table . ' ON ' . $one . ' ' . $operator . ' ' . $two;
        $this->joinTables[] = $joinString;

        return $this;
    }

    /**
     * 组装Where条件
     *
     * @param      $boolean
     * @param      $field
     * @param null $operator
     * @param null $value
     *
     * @return array
     * @throws \Exception
     */
    private function _where($boolean, $field, $operator = null, $value = null)
    {
        $args = func_get_args();

        if (count($args) == 2) {
            $field = $args[0];
            $operator = '=';
            $value = $args[1];
            $where = [$field, $operator, $value];
        } elseif (count($args) == 3) {
            $boolean = $args[0];
            if (($boolean == 'and') || ($boolean == 'or')) {
                $field = $args[1];
                $operator = '=';
                $value = $args[2];
                $where = [$boolean, $field, $operator, $value];
            } else {
                $field = $args[0];
                $operator = $args[1];
                $value = $args[2];
                $where = [$field, $operator, $value];
            }
        } elseif (count($args) == 4) {
            $where = $args;
        } else {
            throw new \Exception('variable count must be 2 or 3', 4001);
        }

        return $where;
    }

    /**
     * 增加查询条件
     *
     * @param string       $field 字段名
     * @param string       $operator 比较运算符
     * @param string|array $value 比较值。IN子句的比较值必须为数组
     *
     * @return $this
     */
    public function where($field, $operator = null, $value = null)
    {
        $where = $this->_where(...func_get_args());
        $this->conditions[] = $where;
        return $this;
    }

    /**
     * 增加查询条件
     *
     * @param string            $field 字段名
     * @param string|null       $operator 比较运算符
     * @param string|array|null $value 比较值。IN子句的比较值必须为数组
     *
     * @return $this
     */
    public function andWhere($field, $operator = null, $value = null)
    {
        $args = func_get_args();
        $args = array_merge(['and'], $args);
        $where = $this->_where(...$args);
        $this->conditions[] = $where;
        return $this;
    }

    /**
     * 增加查询条件
     *
     * @param string            $field 字段名
     * @param string            $operator 比较运算符
     * @param string|array|null $value 比较值。IN子句的比较值必须为数组
     *
     * @return $this
     */
    public function orWhere($field, $operator = null, $value = null)
    {
        $args = func_get_args();
        $args = array_merge(['or'], $args);
        $where = $this->_where(...$args);
        $this->conditions[] = $where;
        return $this;
    }

    /**
     * 扩展查询条件子句。如果使用where，orWhere，andWhere方法无法构造需要的查询条件，可以用此方法手工构造查询条件
     *
     * @param string $append 扩展查询条件子句
     * @param array  $appendBindings 扩展查询条件子句绑定值列表
     *
     * @return $this
     */
    public function appendWhere($append, $appendBindings = [])
    {
        $this->appendConditions[] = $append;
        $this->appendBindings = array_merge($this->appendBindings, $appendBindings);

        return $this;
    }

    /**
     * 增加分组字段
     *
     * @param string|array $fields 分组字段列表
     *
     * @return $this
     */
    public function groupBy($fields)
    {
        if (is_string($fields)) {
            $fields = explode(',', $fields);
        }

        $this->groupBys = array_merge($this->groupBys, $fields);

        return $this;
    }

    /**
     * Having子句
     *
     * @param string            $field 字段名
     * @param string|null       $operator 比较运算符
     * @param string|array|null $value 比较值
     *
     * @return $this
     */
    public function having($field, $operator = null, $value = null)
    {
        $having = $this->_where(...func_get_args());
        $this->havings[] = $having;
        return $this;
    }

    /**
     * 增加Having条件
     *
     * @param string            $field 字段名
     * @param string|null       $operator 比较运算符
     * @param string|array|null $value 比较值。IN子句的比较值必须为数组
     *
     * @return $this
     */
    public function andHaving($field, $operator = null, $value = null)
    {
        $args = func_get_args();
        $args = array_merge(['and'], $args);
        $having = $this->_where(...$args);
        $this->havings[] = $having;
        return $this;
    }

    /**
     * 增加Having条件
     *
     * @param string            $field 字段名
     * @param string            $operator 比较运算符
     * @param string|array|null $value 比较值。IN子句的比较值必须为数组
     *
     * @return $this
     */
    public function orHaving($field, $operator = null, $value = null)
    {
        $args = func_get_args();
        $args = array_merge(['or'], $args);
        $having = $this->_where(...$args);
        $this->havings[] = $having;
        return $this;
    }

    /**
     * 执行更新性的SQL语句并返回影响行数
     *
     * @param string $sql 要执行的SQL语句。
     * @param array  $params 绑定参数
     *
     * @return int 影响的行数
     */
    public function exec($sql, $params = [])
    {
        return $this->connection->exec($sql, $params);
    }

    /**
     * 执行一个查询性的SQL语句并返回结果
     *
     * @param string $sql 要执行的SQL语句
     * @param array  $params 绑定参数
     *
     * @return array 查询结果
     */
    public function query($sql, $params = [])
    {
        return $this->connection->query($sql, $params);
    }

    /**
     * 根据主键id获取一条数据
     *
     * @param string       $id 主键值
     * @param array|string $fields 字段列表
     *
     * @return array
     */
    public function getRowById($id, $fields = '*')
    {
        $this->conditions[] = [$this->primkey, '=', $id];

        return $this->getRow($fields);
    }

    /**
     * 获取某个字段值
     *
     * @param int    $id 主键id
     * @param string $field 字段名
     *
     * @return mixed|string:
     */
    public function getFieldById($id, $field)
    {
        $this->conditions[] = [$this->primkey, '=', $id];
        return $this->getOne($field);
    }

    /**
     * 通过条件获取总数
     *
     * @return int
     */
    public function getCount()
    {
        $field = 'count(*)';
        return $this->getOne($field);
    }

    /**
     * 取得多条记录
     *
     * @param array|string $ids 主键值列表，数组或者逗号分隔的字符串
     * @param array|string $fields 字段列表
     *
     * @return array
     */
    public function getRowsByIds($ids, $fields = '*')
    {
        if (is_string($ids)) {
            $ids = explode(',', $ids);
        }

        if (!$ids) {
            return [];
        }

        $this->conditions[] = [$this->primkey, 'in', $ids];

        return $this->getRows($fields);
    }

    /**
     * 根据主键id更新一条记录
     *
     * @param int   $id 主键值
     * @param array $row 更新数据
     *
     * @return int 影响行数
     */
    public function updateById($id, $row)
    {
        $this->conditions[] = [$this->primkey, '=', $id];
        return $this->update($row);
    }

    /**
     * 删除一条记录
     *
     * @param int $id 记录主键值
     *
     * @return int 影响行数
     */
    public function deleteById($id)
    {
        $this->conditions[] = [$this->primkey, '=', $id];
        return $this->delete();
    }

    /**
     * 获取字段值列表
     *
     * @param string $field 字段名
     *
     * @return array
     */
    public function pluck($field)
    {
        $rows = $this->getRows($field);
        $values = array_column($rows, $field);

        return $values;
    }

    /**
     * 获取一个字段的值
     *
     * @param string $field 字段名
     *
     * @return mixed|string
     */
    public function getOne($field)
    {
        list($sql, $params) = $this->makeQuery($field);
        return $this->connection->getOne($sql, $params);
    }

    /**
     * 获取一行数据
     *
     * @param array|string $fields 字段列表
     *
     * @return array|mixed
     */
    public function getRow($fields = '*')
    {
        list($sql, $params) = $this->makeQuery($fields);
        return $this->connection->getRow($sql, $params);
    }

    /**
     * 查询多行数据
     *
     * @param array|string $fields 字段列表
     * @param string|null  $sort 排序字段
     * @param string|null  $limit
     * @param bool         $primkey 是否主键索引
     *
     * @return array
     */
    public function getRows($fields = '*', $sort = null, $limit = null, $primkey = false)
    {
        list($sql, $params) = $this->makeQuery($fields, $sort, $limit);
        return $this->connection->getRows($sql, $params, $primkey);
    }

    /**
     * 获取最后插入的ID
     *
     * @return int 最后插入的ID
     */
    public function lastInsertId()
    {
        return $this->connection->lastInsertId();
    }

    /**
     * 获取最后执行的SQL
     *
     * @return string  获取最后执行的SQL
     */
    public function getLastSql()
    {
        return $this->connection->getLastSql();
    }

    /**
     * 插入一条记录
     *
     * @param array $row 要插入的数据
     *
     * @return int 影响的行数
     */
    public function insert($row)
    {
        $table = $this->table;

        $pairs = $this->updateBindPairs($row);
        $params = $this->updateBindValues($row);

        $sql = "INSERT INTO {$table} SET {$pairs}";

        return $this->connection->exec($sql, $params);
    }

    /**
     * 替换数据行（主键重复或unique索引字段值重复则替换，否则插入新行）
     *
     * @param array $row 要替换的数据
     *
     * @return int 影响的行数
     */
    public function replace($row)
    {
        $table = $this->table;

        $pairs = $this->updateBindPairs($row);
        $params = $this->updateBindValues($row);

        $sql = "REPLACE INTO {$table} SET {$pairs}";

        return $this->connection->exec($sql, $params);
    }

    /**
     * 删除数据
     *
     * @return int 影响的行数
     */
    public function delete()
    {
        $table = $this->table;

        $sqlWhere = $this->makeWhere();
        $params = $this->makeWhereBindings();
        $this->clear();

        $sql = "DELETE FROM {$table} " . $sqlWhere;

        return $this->connection->exec($sql, $params);
    }

    /**
     * 更新数据
     *
     * @param array $row 要更新的数据
     *
     * @return int 影响行数
     */
    public function update($row)
    {
        $table = $this->table;

        $updatePairs = $this->updateBindPairs($row);
        $updateBindValues = $this->updateBindValues($row);

        $sqlWhere = $this->makeWhere();
        $whereBindValues = $this->makeWhereBindings();

        $params = array_merge($updateBindValues, $whereBindValues);
        $params = array_values($params);

        $this->clear();

        $sql = "UPDATE {$table} SET {$updatePairs} " . $sqlWhere;

        return $this->connection->exec($sql, $params);
    }

    /**
     * 插入一行数据（主键重复或unique索引字段值重复则忽略）
     *
     * @param array $row 要更新的数据
     *
     * @return int 影响行数
     */
    public function insertIgnore($row)
    {
        $table = $this->table;

        $pairs = $this->updateBindPairs($row);
        $params = $this->updateBindValues($row);

        $sql = "INSERT IGNORE INTO {$table} SET {$pairs}";

        return $this->connection->exec($sql, $params);
    }

    /**
     * 字段值自增
     *
     * @param int    $id 主键id
     * @param string $field 自增字段
     * @param int    $incValue 自增值
     *
     * @return int 影响行数
     */
    public function increase($id, $field, $incValue = 1)
    {
        $table = $this->table;

        $primkey = $this->primkey;
        $this->conditions[] = [$primkey, '=', $id];

        $sqlWhere = $this->makeWhere();
        $whereBindValues = $this->makeWhereBindings();

        $sql = "UPDATE {$table} SET {$field} = {$field} + {$incValue} " . $sqlWhere;

        return $this->connection->exec($sql, $whereBindValues);
    }

    /**
     * 字段值自减
     *
     * @param int    $id 主键id
     * @param string $field 自减字段
     * @param int    $decValue 自减值
     *
     * @return int 影响行数
     */
    public function decrease($id, $field, $decValue = 1)
    {
        $table = $this->table;

        $primkey = $this->primkey;

        $this->conditions[] = [$primkey, '=', $id];

        $sqlWhere = $this->makeWhere();
        $whereBindValues = $this->makeWhereBindings();

        $sql = "UPDATE {$table} SET {$field} = IF({$field} > 0, {$field} - {$decValue}, 0) " . $sqlWhere;

        return $this->connection->exec($sql, $whereBindValues);
    }

    /**
     * 读取一个表的全部字段名
     *
     * @param array $excepts 要剔除的字段名列表
     *
     * @return array 字段名数组
     */
    public function getFields($excepts = [])
    {
        $table = $this->table;
        $sql = "describe {$table}";

        $fields = $this->connection->query($sql);
        $fields = array_column($fields, 'Field');

        if ($excepts) {
            $fields = array_diff($fields, $excepts);
            $fields = array_values($fields);
        }

        return $fields;
    }

    /**
     * 开始一个事务
     */
    public function beginTransaction()
    {
        return $this->connection->beginTransaction();
    }

    /**
     * 回滚一个事务
     */
    public function rollBack()
    {
        return $this->connection->rollBack();
    }

    /**
     * 回滚一个事务
     */
    public function commit()
    {
        return $this->connection->commit();
    }

    /**
     * 生成更新语句的参数绑定语句
     *
     * @param array $row 要更新的数据行
     *
     * @return string
     */
    private function updateBindPairs($row)
    {
        $pairs = [];
        foreach ($row as $key => $value) {
            $pairs[] = "{$key} = ?";
        }

        $sql = implode(', ', $pairs);

        return $sql;
    }

    /**
     * 生成更新语句绑定参数
     *
     * @param array $row 要更新的数据行
     *
     * @return array
     */
    private function updateBindValues($row)
    {
        $params = [];
        foreach ($row as $key => $value) {
            $params[] = $value;
        }

        return $params;
    }

    /**
     * 生成连表子句
     *
     * @return string
     */
    private function joinString()
    {
        $joinTables = $this->joinTables;
        $joinString = '';
        if ($joinTables) {
            $joinString = implode(' ', $joinTables);
        }

        return $joinString;
    }

    /**
     * 生成where子句
     *
     * @return string
     */
    private function whereString()
    {
        $conditions = $this->conditions;

        return $this->makeConditionString($conditions);
    }

    /**
     * 生成where子句绑定参数
     *
     * @return array
     */
    private function whereBindValues()
    {
        $conditions = $this->conditions;

        $params = [];
        foreach ($conditions as $condition) {
            $value = end($condition);
            if (is_array($value)) {
                $params = array_merge($params, $value);
            } else {
                $params[] = $value;
            }
        }

        return $params;
    }

    /**
     * 生成扩展查询条件子句
     *
     * @return string
     */
    private function appendString()
    {
        if (empty($this->appendConditions)) {
            return '';
        } else {
            return implode(' ', $this->appendConditions);
        }
    }

    /**
     * 生成扩展查询条件子句绑定值
     *
     * @return array
     */
    private function appendBindValues()
    {
        return $this->appendBindings;
    }

    /**
     * 清理连表、查询条件、扩展查询条件和扩展查询绑定值
     */
    private function clear()
    {
        $this->joinTables = [];
        $this->conditions = [];
        $this->appendConditions = [];
        $this->appendBindings = [];
        $this->groupBys = [];
        $this->havings = [];
    }

    /**
     * 组装SQL查询语句
     *
     * @param array|string $fields 字段列表
     * @param string|null  $sort 排序，形如：id asc
     * @param string|null  $limit LIMIT，形如：0, 100
     *
     * @return array
     */
    private function makeQuery($fields = '*', $sort = null, $limit = null)
    {
        $table = $this->table;

        $sort = $sort ? " ORDER BY $sort" : '';
        $limit = $limit ? ' LIMIT ' . $limit : '';
        $fields = is_array($fields) ? implode(',', $fields) : $fields;

        $sqlWhere = $this->makeWhere();
        $params = $this->makeWhereBindings();
        $sqlJoin = $this->joinString();
        $sqlGroupBy = $this->makeGroupByString();
        $havingString = $this->havingString();
        $havingBindings = $this->havingBindValues();

        $params = array_merge($params, $havingBindings);

        $sql = 'SELECT ' . $fields . ' FROM ' . $table . ' ' . $sqlJoin . ' ' . $sqlWhere . ' ' . $sqlGroupBy . ' ' . $havingString . ' ' . $sort . $limit;

        $this->clear();

        return [$sql, $params];
    }

    /**
     * 组装where语句
     *
     * @return string
     */
    private function makeWhere()
    {
        $sqlWhere = $this->whereString();
        $appendWhere = $this->appendString();

        $where = $sqlWhere . ' ' . $appendWhere;
        $where = trim($where);

        if ($where) {
            $where = 'WHERE ' . $where;
        }

        return $where;
    }

    /**
     * 组装where绑定值
     *
     * @return array
     */
    private function makeWhereBindings()
    {
        $whereBindValues = $this->whereBindValues();
        $appendBindValues = $this->appendBindValues();

        $params = array_merge($whereBindValues, $appendBindValues);
        $params = array_values($params);

        return $params;
    }

    /**
     * 生成having子句
     *
     * @return string
     */
    private function havingString()
    {
        $conditions = $this->havings;

        $havingString = $this->makeConditionString($conditions);
        if ($havingString) {
            $havingString = "HAVING " . $havingString;
        }

        return $havingString;
    }

    /**
     * 生成having子句绑定参数
     *
     * @return array
     */
    private function havingBindValues()
    {
        $conditions = $this->havings;

        $params = [];
        foreach ($conditions as $condition) {
            $value = end($condition);
            if (is_array($value)) {
                $params = array_merge($params, $value);
            } else {
                $params[] = $value;
            }
        }

        return $params;
    }

    /**
     * 组装条件子句
     *
     * @param array $conditions
     *
     * @return string
     */
    private function makeConditionString($conditions = [])
    {
        if (empty($conditions)) {
            return '';
        }

        $pairs = [];
        $first = true;
        foreach ($conditions as $condition) {
            if (count($condition) == 2) {
                $boolean = 'AND';
                $field = $condition[0];
                $operator = '=';
                $value = $condition[1];
            } elseif (count($condition) == 3) {
                $boolean = 'AND';
                $field = $condition[0];
                $operator = $condition[1];
                $value = $condition[2];
            } else {
                $boolean = $condition[0];
                $field = $condition[1];
                $operator = $condition[2];
                $value = $condition[3];
            }

            $holder = '?';
            if (is_array($value)) {
                $count = count($value);
                $holder = array_fill(0, $count, '?');
                $holder = implode(',', $holder);
                $holder = "({$holder})";
            }

            if ($first) {
                //第一个条件前不需要加and或or
                $pairs[] = "{$field} {$operator} {$holder}";
                $first = false;
            } else {
                $pairs[] = "{$boolean} {$field} {$operator} $holder";
            }
        }

        $conditionString = implode(' ', $pairs);

        return $conditionString;
    }

    /**
     * 组装GroupBy子句
     *
     * @return string
     */
    private function makeGroupByString()
    {
        $groupByString = '';

        if ($this->groupBys) {
            $groupByString = "GROUP BY " . implode(', ', $this->groupBys);
        }

        return $groupByString;
    }

    /**
     * 根据类名猜测表名
     *
     * @return string
     */
    private function guessTableName()
    {
        $className = static::class;
        $className = array_pop(explode('\\', $className));
        $className = str_replace('Model', '', $className);
        $tableName = preg_replace('/([A-Z])/', '_$1', $className);

        $segments = explode('_', $tableName);
        if ($segments[0] == '') {
            unset($segments[0]);
        }
        $segments = array_values($segments);
        $tableName = implode('_', $segments);
        return strtolower($tableName);
    }
}