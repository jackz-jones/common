package db

// 数据库类型
const (
	// DBTypeMysql mysql数据库
	DBTypeMysql = "mysql"
	// DBTypeKingBaseMysql kingbase数据库，支持mysql兼容模式
	DBTypeKingBaseMysql = "kingbase_mysql"
	// DBTypeKingBasePgsql kingbase数据库，支持pgsql兼容模式
	DBTypeKingBasePgsql = "kingbase_pgsql"
	// DBTypePostgres postgres数据库
	DBTypePostgres = "postgres"
)

// 数据库默认配置
const (

	// MaxLifetime 最大连接时间，防止使用无效的连接，单位：s
	MaxLifetime = 500

	// MaxIdleConns 空闲连接池中连接的最大数量
	MaxIdleConns = 10

	// MaxOpenConns 数据库连接的最大数量
	MaxOpenConns = 10

	// DefaultStringSize string 类型字段的默认长度
	DefaultStringSize = 256

	// DisableDatetimePrecision 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
	DisableDatetimePrecision = true

	// DontSupportRenameIndex 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
	DontSupportRenameIndex = true

	// DontSupportRenameColumn 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
	DontSupportRenameColumn = true

	// SkipInitializeWithVersion 根据当前 MySQL 版本自动配置
	SkipInitializeWithVersion = false
)
