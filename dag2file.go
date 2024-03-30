package merkledag

import (
	"encoding/json"
	"strings"
)

// Hash to file
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型
	var data []byte
	pathSegments := strings.Split(path, "/")
	rootobj := GetObject(store, hash)

	currentNode := rootobj
	for _, segment := range pathSegments {
		//判断是否是最后一个segment,是则是文件
		if segment == pathSegments[len(pathSegments)-1] {
			data = GetData(store, currentNode)
		}

		for i := 0; i < len(currentNode.Links); i++ {
			if currentNode.Links[i].Name != "" {
				//is tree
				if currentNode.Links[i].Name == segment {
					// find path
					childHash := currentNode.Links[i].Hash
					childobj := GetObject(store, childHash)
					currentNode = childobj
				}
			} else {
				return nil
			}
		}
	}
	return data

}

func GetObject(store KVStore, hash []byte) Object {
	//获取hash对应的Object
	data, _ := store.Get(hash)
	if data == nil {
		return Object{}
	}
	var obj Object
	err := json.Unmarshal(data, &obj)
	if err != nil {
		return Object{}
	}
	return obj
}

func GetData(store KVStore, currentNode Object) []byte {
	// 判断是list还是blob
	if currentNode.Links == nil {
		return currentNode.Data
	} else {
		//获取存在list的数据
		data := GetListData(store, currentNode)
		return data
	}
}

func GetListData(store KVStore, obj Object) []byte {
	var data []byte
	for i := 0; i < len(obj.Links); i++ {
		hash := obj.Links[i].Hash
		currentNode := GetObject(store, hash)
		clip := GetData(store, currentNode)
		data = append(data, clip...)
	}
	return data
}
