package utils

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

func NormalizeEntityConfig(v *viper.Viper, singular string, plural string) error {
	one := v.Get(singular)
	multiple := v.Get(plural)
	if one != nil {
		if multiple != nil {
			return fmt.Errorf("failed to normalize %v,%v multiple definitions", singular, plural)
		}
		v.Set(plural, map[string]interface{}{
			"default": one,
		})
		v.Set(singular, nil)
	} else if multiple != nil {
		if reflect.TypeOf(multiple).Kind() == reflect.Slice {
			newData := map[string]interface{}{}
			for i, item := range multiple.([]interface{}) {
				newData[fmt.Sprintf("%v%v", plural, i)] = item
			}
			v.Set(plural, newData)
		}
	}
	return nil

}

func ViperSubSlice(v *viper.Viper, key string) []*viper.Viper {
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
