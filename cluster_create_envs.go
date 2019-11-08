package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/ghodss/yaml"
)

func extract() map[string]interface{} {
	path := "config.yaml"
	if os.Getenv("CONFIG") != "" {
		path = os.Getenv("CONFIG")
	}

	cfgfile, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("could not read config file %q: %v\n", path, err)
		panic(err)
	}

	cfg := make(map[string]interface{})
	err = yaml.Unmarshal(cfgfile, &cfg)
	if err != nil {
		log.Printf("could not parse config file %q: %v\n", cfg, err)
		panic(err)
	}
	return cfg

}

func main() {
	config := extract()
	clusterConfig := config["Cluster"].(map[string]interface{})

	fmt.Printf("BASE_NAME=%s\n", clusterConfig["Name"])
	fmt.Printf("BUCKET_NAME=benchmarking-%s\n", clusterConfig["Name"])
	fmt.Printf("KOPS_STATE_STORE=s3://benchmarking-%s\n", clusterConfig["Name"])
	fmt.Printf("KOPS_CLUSTER_NAME=%s\n", clusterConfig["KopsClusterName"])
	fmt.Printf("NODE_COUNT=%d\n", int(clusterConfig["NodeCount"].(float64)))
	fmt.Printf("NODE_SIZE=%s\n", clusterConfig["NodeSize"])
	fmt.Printf("ZONE=%s\n", clusterConfig["AvailabilityZone"])

}
