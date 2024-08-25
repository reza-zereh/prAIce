# TA-Lib installation
TALIB_VERSION := 0.4.0
TALIB_ARCHIVE := ta-lib-$(TALIB_VERSION)-src.tar.gz
TALIB_URL := https://github.com/reza-zereh/prAIce/releases/download/v0.0.0/$(TALIB_ARCHIVE)

.PHONY: install-talib
install-talib:
	@echo "Downloading TA-Lib $(TALIB_VERSION)..."
	@wget -q $(TALIB_URL) -O $(TALIB_ARCHIVE)
	@echo "Extracting TA-Lib..."
	@tar -xzf $(TALIB_ARCHIVE)
	@echo "Configuring and installing TA-Lib..."
	@cd ta-lib && \
	./configure --prefix=/usr && \
	make && \
	sudo make install
	@echo "Cleaning up..."
	@rm -rf ta-lib $(TALIB_ARCHIVE)
	@echo "TA-Lib installation complete."
