
TESTS = $(shell find test -name "*_test.js")

test:
	@./node_modules/.bin/mocha $(TESTS) -u bdd --reporter list --timeout 600000s --recursive
.PHONY: test
