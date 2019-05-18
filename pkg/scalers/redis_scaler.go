package scalers

import (
	"context"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/go-redis/redis"
	"k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"strconv"
)

type redisScaler struct {
	metadata redisMetadata
	client redis.Client
}

type redisMetadata struct {
	addr string
	password string

	queue string

	queueThreshold int64
}

const (
	queueSizeName = "queueThreshold"
	queueNameName = "queue"
	redisMetricType = "External"
	defaultRedisQueueThreshold = 20
)



func parseRedisMetadata(metadata map[string]string) (redisMetadata, error) {
	meta := redisMetadata{}
	meta.password = ""
	meta.queueThreshold = defaultRedisQueueThreshold

	// todo parse password
	if metadata["addr"] == "" {
		return meta, errors.New("no redis url given")

	}

	meta.addr = metadata["addr"]

	if metadata[queueNameName] == "" {
		return meta, errors.New("need non empty queue name to monitor addr")
	}

	if val, ok := metadata[queueSizeName]; ok {
		t, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return meta, fmt.Errorf("error parsing %s: %s", queueSizeName, err)
		}
		meta.queueThreshold = t
	}

	return meta, nil


}

// IsActive determines if we need to scale from zero
func (s *redisScaler) IsActive(ctx context.Context) (bool, error){
	cmd := s.client.LLen(s.metadata.queue)
	i := cmd.Val()


	if i > 0 {
		return true, nil
	}
	// todo should we keep engines alive at all times?

	return true, nil
}

// Close closes redis client
func (s *redisScaler) Close() error {
	err := s.client.Close()
	return err
}

func (s *redisScaler) GetMetricSpecForScaling() []v2beta1.MetricSpec {

	return []v2beta1.MetricSpec{
		{
			External: &v2beta1.ExternalMetricSource{
				MetricName: queueSizeName,
				TargetAverageValue: resource.NewQuantity(s.metadata.queueThreshold, resource.DecimalSI),

			},
			Type: redisMetricType,


		},
	}

}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *redisScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error){
	qlN, err  := s.client.LLen(s.metadata.queue).Result()
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, err
	}

	log.Debugf("Redis providing metrics about queue %s total size %v", s.metadata.queue, qlN)

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value: *resource.NewQuantity(int64(qlN), resource.DecimalSI),
		Timestamp: metav1.Now(),
	}
	return append([]external_metrics.ExternalMetricValue{}, metric), nil

}

func NewRedisScaler(resolvedEnv, metadata map[string]string) (Scaler, error){
	redisMetadata, err := parseRedisMetadata(metadata)

	if err != nil {
		return nil, fmt.Errorf("error parsing redis metadata: %s", err)
	}

	redisclient := redis.NewClient(&redis.Options{
		Addr: redisMetadata.addr,
		Password: redisMetadata.password,
		DB: 0,
	})

	_, pongE := redisclient.Ping().Result()

	if pongE != nil {
		return nil, fmt.Errorf("Couldn't connect to redis client %s", pongE)
	}

	// todo url parse?
	return &redisScaler{
		metadata: redisMetadata,
		client: *redisclient,
	}, nil

}