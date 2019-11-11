
cluster: cluster_create_envs.go
	eval $$(go run cluster_create_envs.go); aws s3api create-bucket --bucket $${BUCKET_NAME} --region us-east-1
	eval $$(go run cluster_create_envs.go); aws s3api put-bucket-versioning --bucket $${BUCKET_NAME} --versioning-configuration Status=Enabled
	eval $$(go run cluster_create_envs.go); kops create cluster --state=$${KOPS_STATE_STORE} --node-count=$${NODE_COUNT} --node-size=$${NODE_SIZE} --master-size=t3.medium --master-count=1 --zones=$${ZONE}  --name=$${KOPS_CLUSTER_NAME}
	eval $$(go run cluster_create_envs.go); kops update cluster --state=$${KOPS_STATE_STORE} --name=$${KOPS_CLUSTER_NAME} --yes
	@echo
	@echo You make use \'make cluster_validate\' to check on cluster status

cluster_validate:
	eval $$(go run cluster_create_envs.go); kops validate cluster --state=$${KOPS_STATE_STORE} --name=$${KOPS_CLUSTER_NAME}

cluster_teardown:
	@echo CLUSTER CONFIG:
	@go run cluster_create_envs.go
	@echo
	@read -p "Are you sure you want to delete this cluster? [Y/n] " answer; if [[ "$$answer" == "Y" ]]; then \
	  eval $$(go run cluster_create_envs.go); kops delete cluster --state=$${KOPS_STATE_STORE} --name=$${KOPS_CLUSTER_NAME}; \
	else echo "Aborting..."; \
	fi

testrunner:

.PHONY: cluster cluster_validate cluster_teardown
