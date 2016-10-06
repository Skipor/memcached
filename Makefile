GINKGO=ginkgo -r
GINKGO_OPTIONS=-keepGoing  --randomizeAllSpecs --randomizeSuites --failOnPending  --trace --progress --slowSpecThreshold=1

test: 
	$(GINKGO) $(GINKGO_OPTIONS) -race

test-no-race: 
	$(GINKGO) $(GINKGO_OPTIONS)
