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
// @return db
// @return err
func InitGormDB(dbType, dsn string, modelDst ...interface{}) (db *gorm.DB, err error) {
	switch dbType {
	case DBTypeMysql, DBTypeKingBaseMysql:
		db, err = gorm.Open(mysql.New(mysql.Config{
			DSN:                       dsn,
			SkipInitializeWithVersion: SkipInitializeWithVersion,
			DefaultStringSize:         DefaultStringSize,
			DisableDatetimePrecision:  DisableDatetimePrecision,
			DontSupportRenameIndex:    DontSupportRenameIndex,
			DontSupportRenameColumn:   DontSupportRenameColumn,
		}), &gorm.Config{})
	case DBTypeKingBasePgsql, DBTypePostgres:
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
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
