.PHONY: build
build: build_w build_l build_m

build_m:
	@go build -o kafka-topics-report
build_w:
	@GOOS=windows GOARCH=amd64 go build -o kafka-topics-report.exe
build_l:
	@GOOS=linux GOARCH=amd64 go build -o kafka-topics-report_lin .
