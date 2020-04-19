package utils

import (
	"reflect"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

func ViperSubSlice(v *viper.Viper, key string, allowSingle bool) []*viper.Viper {
	data := v.Get(key)
	if data == nil {
		return nil
	}
	switch reflect.TypeOf(data).Kind() {
	case reflect.Slice:
		var vList []*viper.Viper
		for _, item := range data.([]interface{}) {
			sub := viper.New()
			sub.MergeConfigMap(cast.ToStringMap(item))
			vList = append(vList, sub)
		}
		return vList
	case reflect.Map:
		if allowSingle {
			return []*viper.Viper{v.Sub(key)}
		}
	}
	return nil

}

func ViperSubMap(v *viper.Viper, key string) map[string]*viper.Viper {
	data := v.Get(key)
	if data == nil {
		return nil
	}

	if reflect.TypeOf(data).Kind() == reflect.Map {
		subMap := map[string]*viper.Viper{}
		for key, item := range data.(map[string]interface{}) {
			sub := viper.New()
			sub.MergeConfigMap(cast.ToStringMap(item))
			subMap[key] = sub
		}
		return subMap
	}
	return nil
}
