package scalers

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2beta2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

type weatherScaler struct {
	metadata   *weatherMetadata
	httpClient *http.Client
}

type weatherMetadata struct {
	threshold  int64
	host       string
	preference string
}

type WeatherDataList struct {
	List []*WeatherData `json:"consolidated_weather"`
}

type WeatherData struct {
	MinTemp float64 `json:"min_temp"`
	MaxTemp float64 `json:"max_temp"`
	TheTemp float64 `json:"the_temp"`
}

func NewWeatherScaler(config *ScalerConfig) (Scaler, error) {

	httpClient := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout)

	weatherMetadata, err := parseWeatherMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing weather metadata: %s", err)
	}

	return &weatherScaler{
		metadata:   weatherMetadata,
		httpClient: httpClient,
	}, nil
}

func parseWeatherMetadata(config *ScalerConfig) (*weatherMetadata, error) {

	meta := weatherMetadata{}

	if val, ok := config.TriggerMetadata["threshold"]; ok && val != "" {
		threshold, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("threshold: error parsing threshold %s", err.Error())
		} else {
			meta.threshold = int64(threshold)
		}
	}

	if val, ok := config.TriggerMetadata["host"]; ok {
		_, err := url.ParseRequestURI(val)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %s", err)
		}
		meta.host = val
	} else {
		return nil, fmt.Errorf("no host URI given")
	}
	if config.TriggerMetadata["preference"] == "" {
		return nil, fmt.Errorf("no preference given")
	}
	meta.preference = config.TriggerMetadata["preference"]

	return &meta, nil
}

func (s *weatherScaler) IsActive(ctx context.Context) (bool, error) {
	temperature, err := s.getWeather()
	if err != nil {
		return false, err
	}

	return (int64(temperature)) > s.metadata.threshold, nil
}

func (s *weatherScaler) GetMetricSpecForScaling() []v2beta2.MetricSpec {
	targetMetricValue := resource.NewQuantity(int64(s.metadata.threshold), resource.DecimalSI)
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: kedautil.NormalizeString(fmt.Sprintf("%s", "weather")),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetMetricValue,
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

func (s *weatherScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {

	temp, _ := s.getWeather()

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(int64(temp), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *weatherScaler) Close() error {
	return nil
}

func (s *weatherScaler) getJSONData(out interface{}) error {

	request, err := s.httpClient.Get(s.metadata.host)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &out)
	if err != nil {
		return err
	}
	return nil
}

func (s *weatherScaler) getWeather() (int, error) {

	var temp int

	var wDat WeatherDataList
	err := s.getJSONData(&wDat)

	if err != nil {
		return 100, err
	}

	switch s.metadata.preference {
	case "MinTemp":
		temp = int(wDat.List[0].MinTemp)
	case "MaxTemp":
		temp = int(wDat.List[0].MaxTemp)
	case "TheTemp":
		temp = int(wDat.List[0].TheTemp)
	}

	return temp, nil
}
