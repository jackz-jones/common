package db

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// InitGormDB 初始化 gorm db
// @Title InitGormDB
// @Description: 初始化gorm db
// @param dbType 数据库类型：mysql、postgres、kingbase_mysql、kingbase_pgsql
// @param dsn 数据库连接字符串
// @param modelDst 库表列表
// @param conf 可选的 gorm.Config，若传 nil 则使用默认空配置，业务层可按需自定义（如禁用日志、开启预处理等）
// @return db
// @return err
func InitGormDB(dbType, dsn string, modelDst []interface{}, conf *gorm.Config) (db *gorm.DB, err error) {
	// 若未传入 gorm.Config，使用默认空配置
	if conf == nil {
		conf = &gorm.Config{}
	}

	switch dbType {
	case DBTypeMysql, DBTypeKingBaseMysql:
		db, err = gorm.Open(mysql.New(mysql.Config{
			DSN:                       dsn,
			SkipInitializeWithVersion: SkipInitializeWithVersion,
			DefaultStringSize:         DefaultStringSize,
			DisableDatetimePrecision:  DisableDatetimePrecision,
			DontSupportRenameIndex:    DontSupportRenameIndex,
			DontSupportRenameColumn:   DontSupportRenameColumn,
		}), conf)
	case DBTypeKingBasePgsql, DBTypePostgres:
		db, err = gorm.Open(postgres.Open(dsn), conf)
	default:
		return nil, errors.New("unknown db type")
	}

	if err != nil {
		return nil, err
	}

	sqlDb, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %v", err)
	}

	sqlDb.SetMaxIdleConns(MaxIdleConns)
	sqlDb.SetMaxOpenConns(MaxOpenConns)
	sqlDb.SetConnMaxLifetime(MaxLifetime * time.Second)

	if err = sqlDb.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db connection: %v", err)
	}

	// 自动建表
	err = db.AutoMigrate(modelDst...)
	if err != nil {
		return nil, fmt.Errorf("failed to auto migrate tables: %v", err)
	}

	return db, nil
}
