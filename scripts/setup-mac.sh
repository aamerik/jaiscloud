#!/bin/bash
set -e

# =============================================================================
# LocalCloud Dev Environment Setup — Fresh macOS
# =============================================================================
#
# Usage:
#   chmod +x setup-mac.sh
#   ./setup-mac.sh
#
# This script is idempotent — safe to run multiple times.
# It will skip anything already installed.
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

step=0
total=13

progress() {
    step=$((step + 1))
    echo ""
    echo -e "${GREEN}[$step/$total]${NC} $1"
    echo "───────────────────────────────────────────"
}

check() {
    if command -v "$1" &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} $1 already installed: $(command -v "$1")"
        return 0
    else
        return 1
    fi
}

# =============================================================================
# 1. Homebrew
# =============================================================================
progress "Homebrew"

if check brew; then
    brew update
else
    echo "  Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

    # Add to PATH for Apple Silicon
    if [[ $(uname -m) == "arm64" ]]; then
        echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
        eval "$(/opt/homebrew/bin/brew shellenv)"
    else
        echo 'eval "$(/usr/local/bin/brew shellenv)"' >> ~/.zprofile
        eval "$(/usr/local/bin/brew shellenv)"
    fi
fi

# =============================================================================
# 2. Git
# =============================================================================
progress "Git"

if check git; then
    echo "  $(git --version)"
else
    brew install git
fi

# =============================================================================
# 3. Go
# =============================================================================
progress "Go"

if check go; then
    echo "  $(go version)"
else
    brew install go
fi

# Add Go bin to PATH if not already there
if [[ ":$PATH:" != *":$HOME/go/bin:"* ]]; then
    echo 'export PATH="$HOME/go/bin:$PATH"' >> ~/.zshrc
    export PATH="$HOME/go/bin:$PATH"
    echo "  Added \$HOME/go/bin to PATH"
fi

# =============================================================================
# 4. Node.js (required for Claude Code)
# =============================================================================
progress "Node.js"

if check node; then
    echo "  node $(node --version)"
else
    brew install node
fi

# =============================================================================
# 5. Docker Desktop
# =============================================================================
progress "Docker Desktop"

if check docker; then
    echo "  $(docker --version)"
else
    echo "  Installing Docker Desktop..."
    brew install --cask docker
    echo -e "  ${YELLOW}!${NC} Open Docker Desktop from Applications to complete setup."
    echo "     Docker must be running before you can use full mode."
fi

# =============================================================================
# 6. AWS CLI v2
# =============================================================================
progress "AWS CLI v2"

if check aws; then
    echo "  $(aws --version)"
else
    brew install awscli
fi

# Configure localcloud profile
echo "  Configuring AWS 'localcloud' profile..."
aws configure set aws_access_key_id test --profile localcloud
aws configure set aws_secret_access_key test --profile localcloud
aws configure set region us-east-1 --profile localcloud
echo -e "  ${GREEN}✓${NC} Profile 'localcloud' configured"

# Add awslocal alias if not present
if ! grep -q 'alias awslocal=' ~/.zshrc 2>/dev/null; then
    echo 'alias awslocal="aws --endpoint-url=http://localhost:4566 --profile localcloud"' >> ~/.zshrc
    echo -e "  ${GREEN}✓${NC} Added 'awslocal' alias to ~/.zshrc"
else
    echo -e "  ${GREEN}✓${NC} 'awslocal' alias already exists"
fi

# =============================================================================
# 7. Go Development Tools
# =============================================================================
progress "Go development tools (golangci-lint, gopls, delve, goimports)"

echo "  Installing golangci-lint..."
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest 2>&1 | tail -1 || true

echo "  Installing gopls (Go language server)..."
go install golang.org/x/tools/gopls@latest 2>&1 | tail -1 || true

echo "  Installing delve (debugger)..."
go install github.com/go-delve/delve/cmd/dlv@latest 2>&1 | tail -1 || true

echo "  Installing goimports..."
go install golang.org/x/tools/cmd/goimports@latest 2>&1 | tail -1 || true

echo "  Installing gocov..."
go install github.com/axw/gocov/gocov@latest 2>&1 | tail -1 || true

echo -e "  ${GREEN}✓${NC} All Go tools installed"

# =============================================================================
# 8. CLI Utilities
# =============================================================================
progress "CLI utilities (jq, httpie, watch)"

for tool in jq httpie watch; do
    if brew list "$tool" &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} $tool already installed"
    else
        echo "  Installing $tool..."
        brew install "$tool"
    fi
done

# =============================================================================
# 9. VS Code + Extensions
# =============================================================================
progress "VS Code + Go extension"

if check code; then
    echo "  VS Code already installed"
else
    echo "  Installing VS Code..."
    brew install --cask visual-studio-code
fi

if check code; then
    echo "  Installing VS Code extensions..."
    code --install-extension golang.go --force 2>/dev/null || true
    code --install-extension eamodio.gitlens --force 2>/dev/null || true
    code --install-extension ms-azuretools.vscode-docker --force 2>/dev/null || true
    code --install-extension redhat.vscode-yaml --force 2>/dev/null || true
    echo -e "  ${GREEN}✓${NC} Extensions installed"
else
    echo -e "  ${YELLOW}!${NC} VS Code CLI not found — install extensions manually after opening VS Code"
fi

# =============================================================================
# 10. Claude Code
# =============================================================================
progress "Claude Code"

if check claude; then
    echo "  claude $(claude --version 2>/dev/null || echo 'installed')"
else
    echo "  Installing Claude Code..."
    npm install -g @anthropic-ai/claude-code
fi

echo -e "  ${YELLOW}!${NC} Run 'claude' to authenticate if this is your first time."

# =============================================================================
# 11. kind (local K8s — needed for full mode)
# =============================================================================
progress "kind (local Kubernetes)"

if check kind; then
    echo "  $(kind version)"
else
    brew install kind
fi

# =============================================================================
# 12. Initialize LocalCloud Go project
# =============================================================================
progress "Initialize LocalCloud Go project"

PROJECT_DIR="$HOME/Code/localcloud"

if [ -d "$PROJECT_DIR" ] && [ -f "$PROJECT_DIR/go.mod" ]; then
    echo -e "  ${GREEN}✓${NC} Project already exists at $PROJECT_DIR"
else
    echo "  Creating project at $PROJECT_DIR..."
    mkdir -p "$PROJECT_DIR"
    cd "$PROJECT_DIR"

    # Init git
    if [ ! -d .git ]; then
        git init
    fi

    # Init Go module
    if [ ! -f go.mod ]; then
        go mod init localcloud
    fi

    # Create directory structure (Phase 0)
    mkdir -p cmd/localcloud
    mkdir -p internal/config
    mkdir -p internal/gateway/middleware
    mkdir -p internal/adapter/aws/services
    mkdir -p internal/provider/queue
    mkdir -p internal/store/aws/sqs
    mkdir -p internal/events
    mkdir -p internal/clock
    mkdir -p internal/admin
    mkdir -p tests/integration
    mkdir -p .vscode

    # Install Phase 0 Go dependencies
    echo "  Installing Go dependencies..."
    go get github.com/go-chi/chi/v5
    go get github.com/spf13/cobra
    go get github.com/spf13/viper
    go get github.com/stretchr/testify
    go get github.com/aws/aws-sdk-go-v2
    go get github.com/aws/aws-sdk-go-v2/config
    go get github.com/aws/aws-sdk-go-v2/credentials
    go get github.com/aws/aws-sdk-go-v2/service/sqs

    # Create VS Code settings
    cat > .vscode/settings.json << 'VSCODE_EOF'
{
    "go.lintTool": "golangci-lint",
    "go.lintFlags": ["--fast"],
    "go.useLanguageServer": true,
    "go.testFlags": ["-v", "-count=1"],
    "go.coverOnSave": false,
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "golang.go",
    "[go]": {
        "editor.codeActionsOnSave": {
            "source.organizeImports": "explicit"
        }
    }
}
VSCODE_EOF

    # Create Makefile
    cat > Makefile << 'MAKE_EOF'
.PHONY: build run dev test test-unit test-race test-integration lint fmt clean health reset smoke

build:
	go build -o bin/localcloud ./cmd/localcloud

run: build
	./bin/localcloud start --port 4566 --mode lite

dev:
	go run ./cmd/localcloud start --port 4566 --mode lite --log-level debug

test: test-unit test-integration

test-unit:
	go test ./internal/... -v -count=1

test-race:
	go test -race ./internal/... -count=1

test-integration:
	go test ./tests/integration/ -v -count=1 -timeout 60s

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .
	goimports -w .

clean:
	rm -rf bin/

health:
	@curl -s http://localhost:4566/_localcloud/health | jq .

reset:
	@curl -s -X POST http://localhost:4566/_localcloud/reset | jq .

smoke:
	@echo "Creating queue..."
	@awslocal sqs create-queue --queue-name smoke-test
	@echo "Sending message..."
	@awslocal sqs send-message --queue-url http://localhost:4566/000000000000/smoke-test --message-body "hello"
	@echo "Receiving message..."
	@awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/smoke-test
	@echo "Smoke test passed."
MAKE_EOF

    # Create CLAUDE.md for the Go project
    cat > CLAUDE.md << 'CLAUDE_EOF'
# CLAUDE.md

## What This Project Is

LocalCloud is a multi-cloud local emulator built in Go. Phase 0 implements SQS-only
as a proof of concept. See plan-localcloud-lld-00-poc.md for the full spec.

## Commands

```bash
# Build
go build ./cmd/localcloud

# Run
go run ./cmd/localcloud start --port 4566 --mode lite

# Run tests (requires LocalCloud running on :4566)
go test ./tests/integration/ -v -count=1

# Run unit tests
go test ./internal/... -v

# Run with race detector
go test -race ./...

# Lint
golangci-lint run ./...
```

## Architecture

Request flow: CLI -> Config -> Gateway -> Middleware -> AWS Adapter -> SQS Codec -> QueueProvider -> Store -> Response

Key packages:
- cmd/localcloud/         -- entry point, wiring
- internal/gateway/       -- HTTP server, middleware, request types
- internal/adapter/aws/   -- AWS service detection, SQS codec
- internal/provider/queue/ -- SQS business logic (cloud-agnostic)
- internal/store/         -- ResourceStore interface + memory impl
- internal/store/aws/sqs/ -- SQS message store interface + memory impl
- internal/clock/         -- Clock interface for deterministic mode
- internal/events/        -- EventBus for inter-service communication

## Conventions

- Providers are cloud-agnostic -- never import from internal/adapter/
- Adapters never import from internal/store/ -- they don't touch storage
- Store packages are leaf dependencies -- no internal/ imports
- All time operations use nr.Clock.Now(), never time.Now() directly
- Errors use canonical codes (NotFound, AlreadyExists) -- codecs map to AWS-specific codes
- Tests call POST /_localcloud/reset between test cases
CLAUDE_EOF

    # Create .gitignore
    cat > .gitignore << 'GIT_EOF'
# Binaries
bin/
*.exe
*.dll
*.so
*.dylib

# Test
*.test
*.out
coverage.html

# IDE
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local
GIT_EOF

    echo -e "  ${GREEN}✓${NC} Project initialized at $PROJECT_DIR"
fi

# =============================================================================
# 13. Verification
# =============================================================================
progress "Verifying installation"

echo ""
PASS=0
FAIL=0

verify() {
    if command -v "$1" &> /dev/null; then
        version=$($2 2>&1 | head -1)
        echo -e "  ${GREEN}✓${NC} $1 — $version"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${NC} $1 — NOT FOUND"
        FAIL=$((FAIL + 1))
    fi
}

verify "go"                "go version"
verify "git"               "git --version"
verify "node"              "node --version"
verify "docker"            "docker --version"
verify "aws"               "aws --version"
verify "golangci-lint"     "golangci-lint --version"
verify "gopls"             "gopls version"
verify "dlv"               "dlv version"
verify "goimports"         "goimports -h"
verify "jq"                "jq --version"
verify "code"              "code --version"
verify "claude"            "claude --version"
verify "kind"              "kind version"

echo ""
echo "───────────────────────────────────────────"
echo -e "${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}All tools installed successfully!${NC}"
else
    echo -e "${YELLOW}Some tools failed to install. Check the errors above.${NC}"
fi

echo ""
echo "═══════════════════════════════════════════"
echo " Next steps:"
echo "═══════════════════════════════════════════"
echo ""
echo " 1. Reload your shell:"
echo "      source ~/.zshrc"
echo ""
echo " 2. Authenticate Claude Code (first time only):"
echo "      claude"
echo ""
echo " 3. Open the project:"
echo "      cd ~/Code/localcloud"
echo "      code ."
echo ""
echo " 4. Start implementing Phase 0 (LLD-00):"
echo "      claude \"Implement Step 1 of LLD-00: project skeleton with health endpoint\""
echo ""
echo "═══════════════════════════════════════════"
