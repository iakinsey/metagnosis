VENV_DIR = .env

.PHONY: build archive clean run

build:
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip

archive:
	git ls-files | tar -czf metagnosis.tar.gz --transform 's,^,metagnosis/,' -T -

clean:
	rm -rf $(VENV_DIR)

run: build
	$(VENV_DIR)/bin/python -m metagnosis.main
