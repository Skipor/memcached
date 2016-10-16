GINKGO=ginkgo -r
GINKGO_OPTIONS=-v -keepGoing  --randomizeAllSpecs --randomizeSuites --failOnPending  --trace --progress --slowSpecThreshold=1
BUILD=go build
BUILD_OPTIONS= -o ./out/memcached ./cmd/memcached


all: test-race test-no-race-specific integration-race

test: 
	$(GINKGO)  $(GINKGO_OPTIONS) -skipPackage=integration 

test-race: 
	$(GINKGO)  $(GINKGO_OPTIONS) -skipPackage=integration  -race 

test-no-race-specific:
	$(GINKGO) $(GINKGO_OPTIONS) ./recycle/

integration:
	$(GINKGO) $(GINKGO_OPTIONS) ./integration_test/
	
integration-race: 
	export MEMCACHED_RACE=1; \
	$(GINKGO) $(GINKGO_OPTIONS) ./integration_test/

build:
	$(BUILD) $(BUILD_OPTIONS)

build-race:
	$(BUILD) -race $(BUILD_OPTIONS)

clean:
	rm -rfd ./out

