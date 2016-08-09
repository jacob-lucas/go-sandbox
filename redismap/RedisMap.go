package redismap

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
)

type RedisMap struct {
	Conn redis.Conn
}

func (rm RedisMap) Size() int {
	reply, err := rm.Conn.Do("DBSIZE")
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	i, _ := redis.Int(reply, nil)
	return i
}

func (rm RedisMap) IsEmpty() bool {
	return rm.Size() == 0
}

func (rm RedisMap) ContainsKey(key string) bool {
	reply, err := rm.Conn.Do("EXISTS", key)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	b, _ := redis.Bool(reply, nil)
	return b
}

func (rm RedisMap) Get(key string) string {
	reply, err := rm.Conn.Do("GET", key)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	s, _ := redis.String(reply, nil)
	return s
}

func (rm RedisMap) Put(key, value string) string {
	_, err := rm.Conn.Do("SET", key, value)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	return rm.Get(key)
}

func (rm RedisMap) Remove(key string) bool {
	if !rm.ContainsKey(key) {
		return false
	}
	_, err := rm.Conn.Do("DEL", key)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	return true
}