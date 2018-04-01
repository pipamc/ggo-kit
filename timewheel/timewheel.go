package timewheel

import (
	"container/ring"
	"time"
	"container/list"
)

type TWCallback func([]interface{})

type TimeWheel struct {
	interval       time.Duration
	ticker         *time.Ticker
	callback       TWCallback
	buckets        *ring.Ring
	currentPos     int
	requestChannel chan interface{}
	quitChannel    chan struct{}
}

type bucketItem struct {
	interval time.Duration
	expired int64
	value interface{}
}

func New(interval time.Duration, bucketCount int, callback TWCallback) *TimeWheel {
	tw := &TimeWheel{
		interval: interval,
		buckets: ring.New(bucketCount),
		callback: callback,
		currentPos: 0,
		requestChannel: make(chan interface{}),
		quitChannel: make(chan struct{}),
	}
	tw.buckets.Do(func(item interface{}) {
		item = list.New()
	})
	return tw
}

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval * time.Second)
	go tw.run()
}

// Add element, interval means how frequency should this element execute
// expired means this element expired, -1 means execute forever
func (tw *TimeWheel) Add(interval time.Duration, expired time.Duration, item ...interface{}) {
	if expired != -1 {
		tw.requestChannel <- bucketItem{interval: interval, expired: -1, value: item}
	} else{
		tw.requestChannel <- bucketItem{interval: interval, expired: time.Now().Add(expired).Unix(), value: item}
	}
}

func (tw *TimeWheel) Stop() {
	tw.quitChannel <- struct{}{}
}

func (tw *TimeWheel) run() {
	for {
		select {
		case <- tw.quitChannel:
			close(tw.quitChannel)
			close(tw.requestChannel)
			return
		case item := <- tw.requestChannel:
			if value, ok := item.(bucketItem); ok {
				insertPos := tw.buckets.Move(int(value.interval) % int(tw.interval))
				if itemList, ok := insertPos.Value.(*list.List); ok {
					itemList.PushBack(value)
				}
			}
		case <- tw.ticker.C:
			if tw.callback != nil {
				bucket := tw.buckets.Value.(*list.List)
				var validData []interface{}
				root := bucket.Front()
				if root != nil {
					value := root.Value.(*bucketItem)
					prev := root
					if value.expired == -1 || value.expired >= time.Now().Unix() {
						nextExecutePos := tw.buckets.Move(int(value.interval) % int(tw.interval))
						if itemList, ok := nextExecutePos.Value.(*list.List); ok {
							itemList.PushBack(root.Value)
							validData = append(validData, value.value)
							prev := root
							root = root.Next()
							bucket.Remove(prev)
						}
					}
					root = root.Next()
					bucket.Remove(prev)
				}
				if len(validData) > 0 {
					tw.callback(validData)
				}
			}
			tw.buckets = tw.buckets.Next()
		}
	}
}