# Contributing to BensinDB

Thank you for your interest in contributing to BensinDB! 🎉

## Code of Conduct

Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Go version and OS

### Suggesting Features

Feature requests are welcome! Please include:
- Use case and motivation
- Proposed syntax (if applicable)
- Example usage

### Pull Requests

1. Fork the repo
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass: `go test -v ./tests/...`
6. Run benchmarks: `go test -bench=. ./tests/...`
7. Submit PR with clear description

## Development Guidelines

### Code Style
- Follow standard Go conventions
- Use `gofmt` for formatting
- Add comments for exported functions
- Keep functions small and focused

### Testing
- Add unit tests for new features
- Maintain or improve test coverage
- Add benchmark tests for performance-critical code

### Commit Messages
Use clear, descriptive commit messages:
```
feat: add LIKE operator support
fix: resolve parser panic on empty query
docs: update README with new examples
perf: optimize SELECT query execution
test: add benchmark for JOIN operations
```

## Questions?

Feel free to open an issue for any questions!