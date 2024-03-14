package merkledag

import (
	"hash"
	"encoding/json"
)


type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	var rootHash []byte
	if node.Type() == FILE {
		rootHash,_ = StoreFile (store, node, h)
	}else if node.Type() == DIR {
		rootHash = StoreDir (store, node, h)
	}

	return rootHash
}

func StoreFile (store KVStore, node Node, h hash.Hash) ([]byte, []byte){//返回hash 和 type
	content := node.(File).Bytes()
	t := []byte("blob")
	if node.Size() > 256*1024 {
		obj := Object{}
		t = []byte("list")
		n := node.Size()%256*1024
		m := node.Size()/256*1024
		if m>0 { n++ }
		for i := 0; i < int(n); i++ {
			start := i * 256 * 1024
			end := (i + 1) * 256 * 1024
			if end > len(content) {
				end = len(content)
			}
			content := content[start:end]
			json_data := Object{Data: content}
			value,_ := json.Marshal(json_data)
			h.Reset()
			key := h.Sum(value)
			had,_ :=store.Has(key)
			if  !had {
				store.Put(key, value)
			}
			obj.Data=append(obj.Data,[]byte("blob")...)
			obj.Links=append(obj.Links,Link{Hash:key,Size:end-start})
		}
/*






*/
		//将该文件存入kv中
		json_data := Object{Data:obj.Data,Links: obj.Links}
		value,_ := json.Marshal(json_data)
		h.Reset()
		key := h.Sum(value)
		store.Put(key, value)
		return key,t

	}else{
		json_data := Object{Data: content}
		value,_ := json.Marshal(json_data)
		h.Reset()
		key := h.Sum(value)
		had,_ :=store.Has(key)
		if  !had {
			store.Put(key, value)
		}
		return key,t
	}

}

func StoreDir (store KVStore, node Node, h hash.Hash) []byte{
	tree := Object{
		Links: make([]Link, 0),
		Data:  make([]byte, 0),
	}
	dirNode := node.(Dir)
	it := dirNode.It()//get Iterator
	for it.Next() {
		childnode := it.Node()
		if childnode.Type() == FILE {
			key,t := StoreFile(store, childnode, h)
			tree.Data = append(tree.Data,t...)
			tree.Links = append(tree.Links,Link{ Size:int(childnode.Size()), Hash: key})
			value,_ := json.Marshal(tree)
			h.Reset()
			key = h.Sum(value)
			had,_ :=store.Has(key)
			if  !had {
				store.Put(key, value)
			}
		}else if childnode.Type() ==DIR{
			key := StoreDir(store, childnode, h)
			t := "tree"
			tree.Links = append(tree.Links, Link{Size: int(childnode.Size()),Name: childnode.Name(),Hash: key})
			tree.Data = append(tree.Data,[]byte(t)...)
		}
		value,_ := json.Marshal(tree)
		h.Reset()
		key := h.Sum(value)
		had,_ :=store.Has(key)
		if  !had {
			store.Put(key, value)
		}
		return key
	}
	value,_ := json.Marshal(tree)
	h.Reset()
	key := h.Sum(value)
	had,_ :=store.Has(key)
	if  !had {
		store.Put(key, value)
	}
	return key
}

