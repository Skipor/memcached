GINKGO=ginkgo -r
GINKGO_OPTIONS=--randomizeAllSpecs --randomizeSuites --failOnPending  --trace --progress

test: 
	$(GINKGO) $(GINKGO_OPTIONS) -race

test-no-race: 
	$(GINKGO) $(GINKGO_OPTIONS)
