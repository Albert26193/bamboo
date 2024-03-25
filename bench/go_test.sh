#!/bin/bash

go test -v -bench=BenchmarkGoroutinePut -benchtime=30s
go test -v -bench=BenchmarkGoroutineGet -benchtime=30s
go test -v -bench=BenchmarkGoroutineDelete -benchtime=30s