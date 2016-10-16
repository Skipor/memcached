GINKGO=ginkgo -r
GINKGO_OPTIONS=-v -keepGoing  --randomizeAllSpecs --randomizeSuites --failOnPending  --trace --progress --slowSpecThreshold=1

build:
	go build -o ./out/memcached ./cmd/memcached

test: 
	$(GINKGO)  $(GINKGO_OPTIONS) -skipPackage=integration 

test-race: 
	$(GINKGO)  $(GINKGO_OPTIONS) -skipPackage=integration  -race 

clean:
	rm -rfd ./out

