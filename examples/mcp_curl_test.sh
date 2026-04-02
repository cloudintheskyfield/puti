#!/bin/bash
# MCP server test with curl

SERVER_URL="http://localhost:8001"

# Connect to SSE endpoint in background and save session ID
echo "Connecting to SSE endpoint..."
curl -N ${SERVER_URL}/sse > sse_output.txt &
SSE_PID=$!
echo "SSE connection started with PID ${SSE_PID}"

# Wait for session ID to appear in output
echo "Waiting for session ID..."
for i in {1..10}; do
    sleep 1
    # Try to extract session ID from the file
    if grep -q "session_id" sse_output.txt; then
        SESSION_ID=$(grep -A1 "event: session" sse_output.txt | grep "data:" | sed 's/data: //' | jq -r '.session_id')
        if [ ! -z "$SESSION_ID" ]; then
            break
        fi
    fi
    echo "Still waiting... ($i/10)"
done

if [ -z "$SESSION_ID" ]; then
    echo "Failed to get session ID"
    cat sse_output.txt
    kill $SSE_PID
    exit 1
fi

echo "Got session ID: $SESSION_ID"

# Initialize session
echo -e "\nInitializing session..."
curl -s -X POST "${SERVER_URL}/messages?session_id=${SESSION_ID}" \
    -H "Content-Type: application/json" \
    -d '{
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "0.1",
            "capabilities": {},
            "clientInfo": {
                "name": "curl-test",
                "version": "1.0.0"
            }
        }
    }' | jq .

# Call calculate tool
echo -e "\nCalling calculate tool..."
curl -s -X POST "${SERVER_URL}/messages?session_id=${SESSION_ID}" \
    -H "Content-Type: application/json" \
    -d '{
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "calculate",
            "arguments": {
                "expression": "2 + 3 * 4"
            }
        }
    }' | jq .

# Wait for tool result with longer timeout
echo -e "\nWaiting for tool result via SSE (20 seconds)..."
echo "Check sse_output.txt for events..."
sleep 20

# Show SSE output
echo -e "\nSSE output:"
cat sse_output.txt

# Cleanup
echo -e "\nCleaning up..."
kill $SSE_PID
echo "Done." 