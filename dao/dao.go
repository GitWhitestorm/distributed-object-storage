package dao

import (
	"database/sql"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Object struct {
	ID          int
	Name        string
	Version     int
	Size        int
	Hash        string
	ActivatedAt sql.NullTime
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

var DB *gorm.DB

func init() {
	dsn := "root:aaaa@tcp(127.0.0.1:3307)/object?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	DB = db
}

func getObject(name string, version int) (Object, error) {
	obj := Object{}
	err := DB.Where("name=? AND version = ?", name, version).First(&obj).Error
	return obj, err
}

func SearchLastestVersion(name string) (Object, error) {
	obj := Object{}
	err := DB.Where("name = ?", name).Order("version desc").First(&obj).Error
	return obj, err
}

func GetObject(name string, version int) (Object, error) {
	if version == 0 {
		return SearchLastestVersion(name)
	}
	return getObject(name, version)
}
func PutObject(name string, version int, size int, hash string) error {
	obj := Object{Name: name, Version: version, Size: size, Hash: hash}
	return DB.Create(&obj).Error
}
func AddVersion(name string, version int, size int, hash string) error {
	return PutObject(name, version+1, size, hash)
}

func SearchAllVersions(name string, size int) ([]Object, error) {
	objs := make([]Object, 0)
	err := DB.Where("name = ?", name).Order("version desc").Limit(size).Find(&objs).Error
	return objs, err
}
func DelObject(name string, version int) {
	DB.Delete(&Object{}, "name = ? AND version = ?", name, version)
}

func hasHash(hash string) (bool, error) {
	obj := Object{}
	result := DB.Where("hash = ?", hash).First(&obj)
	row := result.RowsAffected
	err := result.Error
	if err != nil {
		return false, err
	}
	if row <= 0 {
		return false, nil
	}
	return true, nil
}

func SearchHashSize(hash string) (int, error) {
	obj := Object{}
	err := DB.Select("size").Where("hash = ?", hash).First(&obj).Error
	if err != nil {
		return 0, err
	}
	return obj.Size, nil

}
