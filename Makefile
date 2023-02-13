func-start:
	func start --verbose --python

func-start-debug:
	func start --verbose --python --debug

unittest:
	python -m unittest discover -s tests/unittest -p "test_*.py"

test:
	make unittest
