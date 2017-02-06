package counter

import (
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
)

// EnqueueCount ... queue で指定された PullQueue に、key に対しての diff 分の加算をキューイングします
// このキューはを AggregateCount で集計し、永続化することでスケーラブルにカウントを行います
func EnqueueCount(ctx context.Context, queue string, key string, diff int) error {
	t := &taskqueue.Task{
		Method:  "PULL",
		Tag:     key,
		Payload: []byte(strconv.Itoa(diff)),
	}

	_, e := taskqueue.Add(ctx, t, queue)
	if _, e := incrementCachedCount(ctx, queue, key, diff); e != nil {
		log.Debugf(ctx, "aecommons: could not increment value for queue: %s key: %s", queue, key)
	}

	return e
}

// AggregateCount ... 指定された queue, key で EnqueueCount されている全ての diff を集計します
// incrementFunc では、queue, key ごとに集計された diff を Datastore などに永続化します
func AggregateCount(ctx context.Context, queue string, incrementFunc func(ctx context.Context, diffMap map[string]int) error) error {
	diffMap := map[string]int{}
	count := 0

	for {
		tasks, e := taskqueue.Lease(ctx, 1000, queue, 180)
		if e != nil {
			return e
		}
		if len(tasks) == 0 {
			break
		}

		for _, task := range tasks {
			key := task.Tag
			diff, _ := strconv.Atoi(string(task.Payload))

			diffMap[key] += diff
			count++
		}

		if e := taskqueue.DeleteMulti(ctx, tasks, queue); e != nil {
			return e
		}

		if len(tasks) < 1000 {
			break
		}
	}

	log.Debugf(ctx, "Aggregated %d counts.", count)

	var memKeys []string
	for k := range diffMap {
		memKeys = append(memKeys, makeCacheKey(queue, k))
	}
	if e := memcache.DeleteMulti(ctx, memKeys); e != nil {
		log.Debugf(ctx, "aecommons: could not delete memcached buffered counts")
	}

	return incrementFunc(ctx, diffMap)
}

// BufferedCount ... 指定された queue, key で EnqueueCount されている全ての diff の合計を返します
func BufferedCount(ctx context.Context, queue string, key string) (int, error) {
	sum := 0

	if v, e := getCachedCount(ctx, queue, key); e == nil {
		return v, nil
	}

	for {
		tasks, e := taskqueue.LeaseByTag(ctx, 1000, queue, 30, key)
		if e != nil {
			return 0, e
		}
		if len(tasks) == 0 {
			break
		}

		for _, task := range tasks {
			diff, _ := strconv.Atoi(string(task.Payload))

			sum += diff
		}

		if len(tasks) < 1000 {
			break
		}
	}

	if _, e := incrementCachedCount(ctx, queue, key, sum); e != nil {
		log.Debugf(ctx, "aecommons: could not set cache value for queue: %s key: %s", queue, key)
	}

	return sum, nil
}

func makeCacheKey(queue, key string) string {
	return "AEBufferedCount(" + queue + "," + key + ")"
}

func getCachedCount(ctx context.Context, queue, key string) (int, error) {
	return incrementCachedCount(ctx, queue, key, 0)
}

func incrementCachedCount(ctx context.Context, queue, key string, diff int) (int, error) {
	v, e := memcache.Increment(ctx, makeCacheKey(queue, key), int64(diff), 0)
	return int(v), e
}
