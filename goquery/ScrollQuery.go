package goquery
/*
  并发说明：
		1. 单一goroutine 循环查询窗口，将结果集指针写入任务channel
		2. 启动多个goroutine解析数据并写入数据channel
		3. 启动单一goroutine读取数据channel返回结果
 */


import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	//"fmt"
	"io"
	"sync"

	"github.com/olivere/elastic/v7"
)
// goroutine并发数设置
const workers = 3

func ScrollQuery(client *elastic.Client,
		index string,
		boolquery elastic.Query,
		source *elastic.FetchSourceContext ) ([]SourceRecord, error){

	scrollResult := client.Scroll().
		Index(index).
		Query(boolquery).
		FetchSourceContext(source).
		Size(300)

	var recordArray []SourceRecord
	ctx := context.Background()

	// 数据队列和任务队列
	revCH := make(chan *SourceRecord, 2000)
	taskCH := make(chan *elastic.SearchResult)

	var waitGroup sync.WaitGroup

	// 循环scroll windows, 生成待解析任务
	waitGroup.Add(1)
	go func() {
		for {
			results, err := scrollResult.Do(ctx)
			if err == io.EOF {
				waitGroup.Done()
				break    // finish build job queue
			}
			if err != nil {
				fmt.Println("err:", err)
				panic(err)
			}
			// scroll查询结果集
			taskCH <- results
		}
	}()

	// 读取数据通道，生成最终数据队列
	waitGroup.Add(1)
	go func(ch <-chan *SourceRecord) {
		timeout := time.NewTimer(time.Second * 2)
		for {
			select {
			case i := <- ch:
				recordArray = append(recordArray, *i)
			case <-timeout.C:
				waitGroup.Done()
			}
		}
	}(revCH)

	// 工作队列：获取并处理每个查询结果集
	for i := 0; i < workers; i++ {
		waitGroup.Add(1)
		go parseWorker(&waitGroup, revCH, taskCH)
	}
	waitGroup.Wait()
	return recordArray, nil
}

func parseWorker(wg *sync.WaitGroup, ch chan<- *SourceRecord, taskCH <-chan *elastic.SearchResult) {
	timeout := time.NewTimer(time.Second * 2)
	for {
		select {
		case job := <- taskCH:
			for _, hit := range job.Hits.Hits {
				var record SourceRecord
				if err := json.Unmarshal(hit.Source, &record); err == nil {
					ch <- &record
				}
			}
		case <- timeout.C:
			wg.Done()
			break
		}
	}
}
