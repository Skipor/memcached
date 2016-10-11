GINKGO=ginkgo -r
GINKGO_OPTIONS=-v -keepGoing  --randomizeAllSpecs --randomizeSuites --failOnPending  --trace --progress --slowSpecThreshold=1

test: 
	$(GINKGO) $(GINKGO_OPTIONS)

test-race: 
	$(GINKGO) $(GINKGO_OPTIONS) -race
