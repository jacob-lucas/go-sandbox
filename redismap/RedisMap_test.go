package redismap_test

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/garyburd/redigo/redis"
	"github.com/go-sandbox/redismap"
)

var conn, _ = redis.Dial("tcp", ":6379")

func after() {
	conn.Do("FLUSHALL")
}

func TestSizeOnEmptyMap(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 0, r.Size())
}

func TestIsEmptyOnEmptyMap(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	assert.True(t, r.IsEmpty())
}

func TestSizeOnNonEmptyMap(t *testing.T) {
	defer after()
	conn.Do("APPEND", "a", "1")
	conn.Do("APPEND", "b", "2")
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 2, r.Size())
}

func TestIsEmptyOnNonEmptyMap(t *testing.T) {
	defer after()
	conn.Do("APPEND", "a", "1")
	conn.Do("APPEND", "b", "2")
	r := redismap.RedisMap{Conn: conn}
	assert.False(t, r.IsEmpty())
}

func TestSizeCountsKeys(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	conn.Do("APPEND", "a", "1")
	conn.Do("APPEND", "a", "2")
	assert.Equal(t, 1, r.Size())
}

func TestContainsKeyOnExistingKey(t *testing.T) {
	defer after()
	conn.Do("APPEND", "a", "1")
	conn.Do("APPEND", "b", "2")
	r := redismap.RedisMap{Conn: conn}
	assert.True(t, r.ContainsKey("a"))
	assert.True(t, r.ContainsKey("b"))
}

func TestContainsKeyOnNonExistingKey(t *testing.T) {
	defer after()
	conn.Do("APPEND", "a", "1")
	conn.Do("APPEND", "b", "2")
	r := redismap.RedisMap{Conn: conn}
	assert.False(t, r.ContainsKey("c"))
	assert.False(t, r.ContainsKey("d"))
}

func TestGetForExistingKey(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	key := "a"
	conn.Do("APPEND", "a", "1")
	assert.Equal(t, "1", r.Get(key))
}

func TestGetForNonExistingKey(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, "", r.Get("foo"))
}

func TestPutSizeForNewKey(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 0, r.Size())
	r.Put("foo", "bar")
	assert.Equal(t, 1, r.Size())
}

func TestPutSizeForExistingKey(t *testing.T) {
	defer after()
	conn.Do("APPEND", "foo", "bar")
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 1, r.Size())
	r.Put("foo", "baz")
	assert.Equal(t, 1, r.Size())
}

func TestPutValueForNewKey(t *testing.T) {
	defer after()
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 0, r.Size())
	val := r.Put("foo", "bar")
	assert.Equal(t, "bar", val)
}

func TestPutValueForExistingKey(t *testing.T) {
	defer after()
	conn.Do("APPEND", "foo", "bar")
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 1, r.Size())
	val := r.Put("foo", "baz")
	assert.Equal(t, "baz", val)
}

func TestRemoveValueForExistingKey(t *testing.T) {
	defer after()
	key := "foo"
	conn.Do("APPEND", key, "bar")
	r := redismap.RedisMap{Conn: conn}
	assert.False(t, r.IsEmpty())
	assert.True(t, r.Remove(key))
	assert.True(t, r.IsEmpty())
}

func TestRemoveValueForNonExistingKey(t *testing.T) {
	defer after()
	conn.Do("APPEND", "foo", "bar")
	r := redismap.RedisMap{Conn: conn}
	assert.Equal(t, 1, r.Size())
	assert.False(t, r.Remove("baz"))
	assert.Equal(t, 1, r.Size())
}
