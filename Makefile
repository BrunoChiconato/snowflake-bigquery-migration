#!/usr/bin/bash

.PHONY: help clean linter test test-cov

default: help

help:
	@echo "Available commands:"
	@echo "- make clean: Remove __pycache__ and .ruff_cache dirs and .pyc files"
	@echo "- make linter: Run linter and formatter (Ruff) on the project"


clean:
	@echo "--> Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

linter:
	@echo "--> Linting and formatting code with Ruff..."
	ruff check . --fix
	ruff format .